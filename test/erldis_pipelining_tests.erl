-module(erldis_pipelining_tests).

-include_lib("eunit/include/eunit.hrl").
-include("erldis.hrl").

pipelining_test()->
  {ok, Client} = erldis:connect("localhost", 6379),
  ?assertEqual(erldis:flushdb(Client), ok),
  erldis:set_pipelining(Client, true),
  erldis:get(Client, <<"pippo">>),
  erldis:set(Client, <<"hello">>, <<"kitty!">>),
  erldis:get(Client, <<"hello">>),
  erldis:setnx(Client, <<"foo">>, <<"bar">>),
  erldis:setnx(Client, <<"foo">>, <<"bar">>),
  ?assertEqual([nil, ok, [<<"kitty!">>],true, false],erldis:get_all_results(Client)).