%% -*- mode: erlang; erlang-indent-level: 4 -*-
%% @doc
%% @end

-module(pects_eunit).

-include_lib("eunit/include/eunit.hrl").

basic_test_() ->
    {inorder,
     {"basic happy testing.",
      {foreach,
       fun start/0,
       fun stop/1,
       [fun t_match/1,
        fun t_init/1,
        fun t_lookup/1,
        fun t_delete/1,
        fun t_info/1,
        fun t_bump/1,
        fun t_reset/1,
        fun t_stress/1
       ]}
     }}.

start() ->
    os:cmd("rm -rf /tmp/pects/foo"),
    {ok, foo} = pects:init(foo, "/tmp/pects"),
    pects:write(foo, key, 12),
    pects:write(foo, {a, a}, [{a, "A"},{b, "B"}]),
    pects:write(foo, {a, b}, #{a => "A",b => "B"}),
    pects:write(foo, {a, c}, #{a => "A",b => "C"}),
    pects:write(foo, {a, d}, {a, d}).

stop(_) ->
    case pects:delete(foo) of
        {ok, foo} -> ok;
        {error, {no_such_table, _}} -> ok
    end.

t_init(_) ->
    [?_assertMatch(
        {error, {exists, _}},
        pects:init(foo, "")),
     fun() ->
            pects:delete(foo),
            ?assertMatch(
               {error,{cannot_create_dir,{not_writable,enoent}}},
               pects:init(foo, ""))
    end].

t_match(_) ->
    [?_assertMatch(
        [{{a, a}, [_, _]}],
        pects:match(foo, '_', [{a, "A"}])),
     ?_assertMatch(
        [{{a, d}, {a, d}}],
        pects:match(foo, '_', {a,'_'})),
     ?_assertMatch(
        [{{a, b}, #{}}, {{a, c}, #{}}],
        pects:match(foo, '_', #{a => "A"})),
     ?_assertMatch(
        [{{a, b}, #{}}, {{a, c}, #{}}],
        pects:match(foo, {a,'_'}, #{a => "A"}))].

t_lookup(_) ->
    [?_assertMatch(
        [{{a, b}, #{}}],
        pects:read(foo, {a,b}))].

t_delete(_) ->
    [?_assertMatch(
        [{key, 12}],
        pects:read(foo, key)),
     ?_assertMatch(
        [{key, 12}],
        pects:delete(foo, key)),
     ?_assertMatch(
        [],
        pects:delete(foo, key)),
     ?_assertMatch(
        [],
        pects:read(foo, key))].

t_info(_) ->
    [?_assertMatch(
        true,
        pects:exists(foo)),
    ?_assertMatch(
        false,
        pects:exists(foo0)),
    ?_assertMatch(
        {ok,#{}},
        pects:info(foo)),
    ?_assertMatch(
        {error, no_such_table},
        pects:info(foo0))].

t_bump(_) ->
    [?_assertMatch(
        [13],
        pects:bump(foo, key))].

t_reset(_) ->
    [fun() ->
             Ref = monitor(process, foo),
             foo ! quit,
             receive
                 {'DOWN', Ref, process, _, _} -> ok
             end,
             {ok, foo} = pects:init(foo, "/tmp/pects"),
             ?assertMatch(
                [{{a, b}, #{}}],
                pects:read(foo, {a,b}))
     end].

t_stress(_) ->
    Recurse = fun(_, _, N) when N < 0 -> ok;
                 (G, P, N) -> P(), G(G, P, N-1)
              end,
    Stress =
        fun() ->
                pects:init(foo, "/tmp/pects"),
                pects:write(foo, k, v),
                pects:read(foo, k)
        end,
    Spawnee = fun(_) -> Recurse(Recurse, Stress, 3000) end,
    [fun() -> tst:pmap(Spawnee, lists:seq(1, 100)) end].
