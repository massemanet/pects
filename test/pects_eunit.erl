%% -*- mode: erlang; erlang-indent-level: 4 -*-
%% @doc
%% @end

-module(pects_eunit).

-include_lib("eunit/include/eunit.hrl").

basic_test_() ->
  {"basic happy testing.",
   {foreach,
    fun start/0,
    fun stop/1,
    [fun t_match/1,
     fun t_init/1,
     fun t_lookup/1,
     fun t_delete/1
    ]
   }}.

start() ->
    os:cmd("rm -rf /tmp/pects/foo"),
    {ok, foo} = pects:init(foo, "/tmp/pects"),
    pects:write(foo, key, value),
    pects:write(foo, {a, a}, [{a, "A"},{b, "B"}]),
    pects:write(foo, {a, b}, #{a => "A",b => "B"}),
    pects:write(foo, {a, c}, #{a => "A",b => "C"}),
    pects:write(foo, {a, d}, {a, d}).

stop(_) ->
    {ok, foo} = pects:delete(foo).

t_init(_) ->
    [?_assertMatch(
        {error, {cannot_create_dir,{not_writable,enoent}}},
        pects:init(foo, ""))].

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
        [{key, value}],
        pects:read(foo, key)),
     ?_assertMatch(
        [{key, value}],
        pects:delete(foo, key)),
     ?_assertMatch(
        [],
        pects:delete(foo, key)),
     ?_assertMatch(
        [],
        pects:read(foo, key))].
