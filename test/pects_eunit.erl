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
    pects:init(foo, "/tmp/pects"),
    pects:write(foo, {a,b}, #{a => "A",b => "B"}),
    pects:write(foo, {a,c}, #{a => "A",b => "C"}).

stop(_) ->
    pects:delete(foo).

t_init(_) ->
    [?_assertMatch({error, exists},
                   pects:init(foo, ""))].

t_match(_) ->
    [?_assertMatch([{{a,_},#{}},{{a,_},#{}}],
                   pects:match(foo, '_', #{a => "A"})),
     ?_assertMatch([{{a,_},#{}},{{a,_},#{}}],
                   pects:match(foo, {a,'_'}, #{a => "A"}))].

t_lookup(_) ->
    [?_assertMatch([{{a,b}, #{}}],
                   pects:read(foo, {a,b}))].

t_delete(_) ->
    pects:delete(foo,{a,b}),
    [?_assertMatch([],
                   pects:read(foo, {a,b}))].
