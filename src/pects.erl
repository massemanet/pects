-module(pects).

-export([init/2, read/2, write/3, delete/1, delete/2, match/3]).

%%-----------------------------------------------------------------------------
%% API
init(Tab, Dir) ->
    case ets:info(Tab, size) of
        undefined ->
            ets:new(Tab, [named_table, ordered_set]),
            ets:insert(Tab, {{meta, data_dir}, Dir}),
            ok = filelib:ensure_dir(data_file(Tab, dummy));
        _ ->
            {error, exists}
    end.

read(Tab, Key) ->
    case ets:lookup(Tab, {data, Key}) of
        [] -> [];
        [{_, _, [Val]}] -> [{Key, Val}]
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

delete(Tab) ->
    Source = data_dir(Tab),
    Dest = Source++"_",
    file:rename(Source, Dest),
    catch ets:delete(Tab),
    spawn(fun() -> rmrf(Dest) end),
    ok.

delete(Tab, Key) ->
    try lock(Tab, Key) of
        [Val] ->
            file:delete(data_file(Tab, Key)),
            ets:delete(Tab, {data, Key}),
            [{Key, Val}];
        [] ->
            []
    catch
        throw:Abort -> {aborted, Abort}
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
        R -> error({error_many_rows, {Tab, element(1, Old), R}})
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

rmrf(F) ->
    case file:list_dir(F) of
        {ok, Fs} ->
            lists:foreach(fun rmrf/1, [filename:join(F,E) || E <- Fs]),
            file:del_dir(F);
        {error, enotdir} ->
            file:delete(F);
        {error, _} ->
            ok
    end.

data_file(Tab, Key) ->
    Name = integer_to_list(erlang:phash2(Key, 4294967296)),
    filename:join(data_dir(Tab), Name).

data_dir(Tab) ->
    [{_, Dir}] = ets:lookup(Tab, {meta, data_dir}),
    filename:join(Dir, Tab).
