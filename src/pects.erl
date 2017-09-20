-module(pects).

-export([init/2, read/2, write/3, delete/1, delete/2, match/3]).

%%-----------------------------------------------------------------------------
%% API
init(Tab, Dir) ->
    case create_table(Tab) of
        {ok, TmpTab} ->
            case populate_and_switch(Tab, TmpTab, Dir) of
                Tab ->
                    {ok, Tab};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

delete(Tab) ->
    case delete_table(Tab) of
        {ok, BaseDir} ->
            delete_files(BaseDir),
            {ok, Tab};
        {error, Error} ->
            {error, Error}
    end.

write(Tab, Key, Val) ->
    try
        OldVal = lock(Tab, {data, Key}),
        case persist(Tab, {data, Key}, Val) of
            ok ->
                unlock(Tab, {data, Key}, OldVal, [Val]);
            {error, Err} ->
                unlock(Tab, {data, Key}, OldVal, OldVal),
                error({error_persisting, {Tab, Key, Err}})
        end
    catch
        throw:Abort -> {aborted, Abort}
    end.

delete(Tab, Key) ->
    try lock(Tab, {data, Key}) of
        [Val] ->
            file:delete(data_file(Tab, {data, Key})),
            ets:delete(Tab, {data, Key}),
            [{Key, Val}];
        [] ->
            ets:delete(Tab, {data, Key}),
            []
    catch
        throw:Abort -> {aborted, Abort}
    end.

read(Tab, Key) ->
    case ets:lookup(Tab, {data, Key}) of
        [] -> [];
        [{_, _, [Val]}] -> [{Key, Val}]
    end.

match(Tab, K, V) ->
    case K of
        '_' ->
            ets:foldr(mk_matchf(V), [], Tab);
        _ ->
            Matches = ets:select(Tab, [{{{data, K}, '_', '_'}, [], ['$_']}]),
            lists:foldr(mk_matchf(V), [], Matches)
    end.

%%----------------------------------------------------------------------------
%% init implementation

delete_table(Tab) ->
    Ref = monitor(process, Tab),
    catch Tab ! quit,
    receive
        {'DOWN', Ref, process, _, {ok, BaseDir}} ->
            {ok, BaseDir};
        {'DOWN', Ref, process, _, Info} ->
            {error, {no_such_table, Info}}
    end.

create_table(Tab) ->
    Self = self(),
    {Pid, Ref} = spawn_monitor(fun() -> owner(Tab, Self) end),
    receive
        {'DOWN', Ref, process, Pid, Info} ->
            {error, {exists, Info}};
        {'UP', Pid, TmpTab} ->
            demonitor(Ref, [flush]),
            {ok, TmpTab}
    end.

owner(Tab, Daddy) ->
    TmpTab = list_to_atom(atom_to_list(Tab)++"_pects_tmp"),
    try
        ets:new(TmpTab, [named_table, ordered_set, public]),
        register(Tab, self())
    catch
        _:_ -> exit({error, {cannot_register, Tab}})
    end,
    Daddy ! {'UP', self(), TmpTab},
    receive
        quit -> exit({ok, base_dir(Tab)})
    end.

populate_and_switch(Tab, TmpTab, RootDir) ->
    case create_dir(Tab, RootDir) of
        {true, BaseDir} ->
            ets:insert(TmpTab, {{meta, dir}, unlocked, BaseDir}),
            persist(TmpTab, {meta, dir}, BaseDir),
            switch_tables(TmpTab, Tab);
        {false, BaseDir} ->
            populate(BaseDir, TmpTab),
            switch_tables(TmpTab, Tab);
        {error, Error} ->
            {error, {cannot_create_dir, Error}}
    end.

populate(Dir, Tab) ->
    fold_dir(filename:join(Dir, data), mk_populatef(Tab)).

mk_populatef(Tab) ->
    fun({regular, File}) ->
            {ok, B} = file:read_file(File),
            {Key, Val} = binary_to_term(B),
            ets:insert(Tab, {Key, unlocked, [Val]});
       ({_, _}) ->
            ok
    end.

switch_tables(From, To) ->
    try
        ets:rename(From, To)
    catch
        _:Error ->
            catch ets:delete(From),
            {error, {switching_failed, Error}}
    end.
%%----------------------------------------------------------------------------
%% locking implementation

lock(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        [] -> lock_new(Tab, Key);
        [{_, unlocked, Old}] -> lock_old(Tab, Key, Old);
        [{_, locked, _}] -> throw(locked);
        Err -> error({error_lookup,Tab, Key, Err})
    end.

lock_new(Tab, Key) ->
    case ets:insert_new(Tab, {Key, locked, []}) of
        true -> [];
        false -> throw(collision)
    end.

lock_old(Tab, Key, Val) ->
    case cas(Tab, {Key, unlocked, Val}, {Key, locked, Val}) of
        true -> Val;
        false -> throw(collision)
    end.

unlock(Tab, Key, Old, New) ->
    case cas(Tab, {Key, locked, Old}, {Key, unlocked, New}) of
        true -> ok;
        false -> error({error_unlocking, {Tab, Key}})
    end.

cas(Tab, Old, New) ->
    case ets:select_replace(Tab, [{Old, [], [{const, New}]}]) of
        1 -> true;
        0 -> false;
        R -> error({error_many_rows, {Tab, Old, R}})
    end.

%%----------------------------------------------------------------------------
%% matching implementation

mk_matchf(V) ->
    fun(E, Acc) -> matchf(E, Acc, V) end.

matchf(E, Acc, V) ->
    try
        {{data, Key}, _, [Val]} = E,
        match(V, Val),
        [{Key, Val}|Acc]
    catch
        _:_ -> Acc
    end.

match('_', _) ->
    true;
match(V, V) ->
    true;
match(A, B) when is_map(A), is_map(B) ->
    maps:filter(mk_mapf(B), A);
match(A, B) when is_list(A), is_list(B) ->
    lists:foreach(mk_listf(B), A);
match(A, B) when is_tuple(A), is_tuple(B) ->
    match(tuple_to_list(A), tuple_to_list(B)).

mk_mapf(M) ->
    fun(K, V) -> match(V, maps:get(K, M)) end.

mk_listf(L) ->
    fun(E) -> take_first(fun(F) -> match(E, F) end, L) end.

take_first(F, []) ->
    error({take_first_fail, F});
take_first(F, [T|Ts]) ->
    try F(T)
    catch _:_ -> take_first(F, Ts)
    end.

%%----------------------------------------------------------------------------
%% persistence implementation

create_dir(Tab, RootDir) ->
    BaseDir = filename:join(RootDir, Tab),
    case filelib:is_dir(BaseDir) of
        true ->
            delete_defunct(BaseDir),
            {false, BaseDir};
        false ->
            case filelib:ensure_dir(filename:join([BaseDir, data, x])) of
                ok -> {true, BaseDir};
                {error, Error} -> {error, {not_writable, Error}}
            end
    end.

persist(Tab, Key, Val) ->
    File = data_file(Tab, Key),
    filelib:ensure_dir(File),
    file:write_file(File, term_to_binary({Key, Val})).

delete_files(BaseDir) ->
    Source = filename:join(BaseDir, data),
    Dest = Source++"_defunct",
    case file:rename(Source, Dest) of
        ok -> rm_rf(BaseDir);
        {error, _} -> ok
    end.

delete_defunct(BaseDir) ->
    Source = filename:join(BaseDir, data),
    Dest = Source++"_defunct",
    rm_rf(Dest).

rm_rf(F) ->
    fold_dir(F, fun deletef/1).

deletef({regular, F}) -> file:delete(F);
deletef({directory, D}) -> file:del_dir(D).

fold_dir(File, Fun) ->
    case file:list_dir(File) of
        {ok, Fs} ->
            AbsFs = [filename:join(File, F) || F <- Fs],
            lists:foreach(fun(F) -> fold_dir(F, Fun) end, AbsFs),
            Fun({directory, File});
        {error, enotdir} ->
            Fun({regular, File});
        {error, _} ->
            ok
    end.

data_file(Tab, Key) ->
    filename:join([base_dir(Tab), data|hashed(Key)]).

hashed(Term) ->
    Hash = crypto:hash(sha256, term_to_binary(Term)),
    [A, B, C, D|R] = [if X<10 -> X+$0; true -> X+$W end || <<X:5>> <= Hash],
    [[A, B], [C, D], R].

base_dir(Tab) ->
    try
        [{_, _, Dir}] = ets:lookup(Tab, {meta, dir}),
        Dir
    catch _:_ -> exit({error, {base_dir, Tab}})
    end.
