-module(erldis_pipelining_tests).

-include_lib("eunit/include/eunit.hrl").
-include("erldis.hrl").

pipelining_test()->
  {ok, Client} = erldis:connect("localhost", 6379),
  erldis:flushall(Client),
  erldis:set_pipelining(Client, true),
  erldis:get(Client, <<"pippo">>),
  erldis:set(Client, <<"hello">>, <<"kitty!">>),
  erldis:setnx(Client, <<"foo">>, <<"bar">>),
  erldis:setnx(Client, <<"foo">>, <<"bar">>),
  [ok, nil, ok, true, false] = erldis:get_all_results(Client).