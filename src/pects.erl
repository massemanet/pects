-module(pects).

-export([init/2, read/2, write/3, delete/1, delete/2, match/3]).

%%-----------------------------------------------------------------------------
%% API
init(Tab, Dir) ->
    case mk_tmp(Tab) of
        {ok, TmpTab} ->
            case populate_and_switch(Tab, TmpTab, Dir) of
                ok ->
                    {ok, Tab};
                Error ->
                    ets:delete(TmpTab),
                    Error
            end;
        Error ->
            Error
    end.

delete(Tab) ->
    case mk_tmp(Tab) of
        {ok, TmpTab} ->
            Source = data_dir(Tab),
            Dest = Source++"_defunct",
            catch ets:delete(Tab),
            case file:rename(Source, Dest) of
                ok -> rm_rf(Dest);
                {error, _} -> ok
            end,
            ets:delete(TmpTab),
            ok;
        Error ->
            Error
    end.

write(Tab, Key, Val) ->
    try
        OldVal = lock(Tab, Key),
        case persist(Tab, Key, Val) of
            ok ->
                unlock(Tab, Key, OldVal, [Val]);
            {error, Err} ->
                unlock(Tab, Key, OldVal, OldVal),
                error({error_persisting, {Tab, Key, Err}})
        end
    catch
        throw:Abort -> {aborted, Abort}
    end.

delete(Tab, Key) ->
    try lock(Tab, Key) of
        [Val] ->
            file:delete(data_file(Tab, Key)),
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

mk_tmp(Tab) ->
    TmpTab = list_to_atom(atom_to_list(Tab)++"_pects_tmp"),
    try
        {ok, ets:new(TmpTab, [named_table, ordered_set, public])}
    catch
        _:_ -> {error, exists}
    end.

populate_and_switch(Tab, TmpTab, RootDir) ->
    case create_dir(Tab, RootDir) of
        {true, BaseDir} ->
            switch_tables(TmpTab, Tab);
        {false, BaseDir} ->
            populate(BaseDir, TmpTab),
            switch_tables(TmpTab, Tab);
        {error, Error} ->
            {error, {cannot_create_dir, Error}}
    end.

create_dir(Tab, RootDir) ->
    BaseDir = filename:join(RootDir, Tab),
    case filelib:ensure_dir() of
        ok ->
            case filelib:is_regular(DefunctFile) of
                true ->
                    rmrf(Dir),
                    ok;
                false ->
                    {error, Error} -> {error, {not_writable, Error}}
    end.

populate(Dir, Tab) ->
    fold_dir(Dir, mk_regf(Tab), fun(_) -> ok end).

mk_regf(Tab) ->
    fun(File) ->
            {ok, B} = file:read_file(File),
            {Key, Val} = binary_to_term(B),
            ets:insert(Tab, {{data, Key}, unlocked, [Val]})
    end.

switch_tables(Tab, TmpTab) ->
    try
        ets:rename(TmpTab, Tab)
    catch
        _:Error ->
            catch ets:delete(TmpTab),
            {error, {switching_failed, Error}}
    end.
%%----------------------------------------------------------------------------
%% locking implementation

lock(Tab, Key) ->
    case ets:lookup(Tab, {data, Key}) of
        [] -> lock_new(Tab, Key);
        [{_, unlocked, Old}] -> lock_old(Tab, Key, Old);
        [{_, locked, _}] -> throw(locked);
        Err -> error({error_lookup,Tab, Key, Err})
    end.

lock_new(Tab, Key) ->
    case ets:insert_new(Tab, {{data, Key}, locked, []}) of
        true -> [];
        false -> throw(collision)
    end.

lock_old(Tab, Key, Val) ->
    case cas(Tab, {{data, Key}, unlocked, Val}, {{data, Key}, locked, Val}) of
        true -> Val;
        false -> throw(collision)
    end.

unlock(Tab, Key, Old, New) ->
    case cas(Tab, {{data, Key}, locked, Old}, {{data, Key}, unlocked, New}) of
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
    fun({{data, Key}, _, [Val]}, Acc) ->
            try
                match(V, Val),
                [{Key, Val}|Acc]
            catch
                _:_ -> Acc
            end;
       (_, Acc) ->
            Acc
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

persist(Tab, Key, Val) ->
    file:write_file(data_file(Tab, Key), term_to_binary({Key, Val})).

rm_rf(F) ->
    fold_dir(F, fun file:delete/1, fun file:del_dir/1).

fold_dir(File, RegF, DirF) ->
    case file:list_dir(File) of
        {ok, Fs} ->
            AbsFs = [filename:join(File, F) || F <- Fs],
            lists:foreach(fun(F) -> fold_dir(F, RegF, DirF) end, AbsFs),
            DirF(File);
        {error, enotdir} ->
            RegF(File);
        {error, _} ->
            ok
    end.

data_file(Tab, Key) ->
    Name = integer_to_list(erlang:phash2(Key, 4294967296)),
    filename:join(data_dir(Tab), Name).

data_dir(Tab) ->
    filename:join([base_dir(Tab), Tab, data]).

meta_dir(Tab) ->
    filename:join([base_dir(Tab), Tab, meta]).

base_dir(Tab) ->
    [{_, Dir}] = ets:lookup(Tab, {meta, dir}),
    Dir.
