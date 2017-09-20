%% -*- mode: erlang; erlang-indent-level: 2 -*-
%% @doc
%% @end

-module(tst).

-export([pmap/2]).


pmap(Fun, Es) ->
    PidRefs = [spawn_monitor(fun() -> Fun(E) end) || E <- Es],
    RefVals = pmp(PidRefs, []),
    RefVals.

pmp([], A) -> A;
pmp(PidRefs, A) ->
    receive
        {'DOWN', Ref, _, Pid, I} -> pmp(PidRefs--[{Pid, Ref}], [{Ref, I}|A])
    end.
