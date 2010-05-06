-module(erldis_pubsub_tests).

-include_lib("eunit/include/eunit.hrl").
-include("erldis.hrl").

publish_basic_test()->
  {ok, Client} = erldis:connect("localhost", 6379),
  % no one follows -> returns 0
  ?assertEqual(0, erldis:publish(Client, <<"foo">>, <<"bar">>)),
  {ok, Client2} = erldis:connect("localhost", 6379),
  ?assertEqual(1, erldis:subscribe(Client2, <<"foo">>, self())),
  ?assertEqual({<<"foo">>,0}, erldis:unsubscribe(Client2, <<"foo">>)).

subscribe_basic_test()->
  {ok, Client} = erldis:connect("localhost", 6379),
  ?assertEqual(1, erldis:subscribe(Client, <<"foo">>, self())),
  ?assertEqual(2, erldis:subscribe(Client, <<"bar">>, self())).

unsubscribe_basic_test()->
  {ok, Client} = erldis:connect("localhost", 6379),
  ?assertEqual(1, erldis:subscribe(Client, <<"foo1">>, self())),
  ?assertEqual({<<"foo1">>,0}, erldis:unsubscribe(Client, <<"foo1">>)),
  ?assertEqual(1, erldis:subscribe(Client, <<"foo1">>, self())),
  ?assertEqual(2, erldis:subscribe(Client, <<"foo2">>, self())),
  ?assertEqual({<<"foo1">>,1}, erldis:unsubscribe(Client)).

pubsub_test()->
  {ok, Pub} = erldis:connect("localhost", 6379),
  {ok, Sub} = erldis:connect("localhost", 6379),
  ?assertEqual(1, erldis:subscribe(Sub, <<"bidule">>, self())),
  ?assertEqual(1, erldis:publish(Pub, <<"bidule">>, <<"bar">>)),
  receive
    {message, <<"bidule">>, Message} ->
      ?assertEqual(<<"bar">>, Message)
  after
    100 ->
      ?assertEqual(true, false) %TODO not proud
  end.
  
pubsub_multiple_test()->
  {ok, Pub} = erldis:connect("localhost", 6379),
  {ok, Sub} = erldis:connect("localhost", 6379),
  ?assertEqual(1, erldis:subscribe(Sub, <<"multibidule">>, self())),
  ?assertEqual(1, erldis:publish(Pub, <<"multibidule">>, <<"bar">>)),
  receive
    {message, <<"multibidule">>, M1} ->
      ?assertEqual(<<"bar">>, M1)
  after
    100 ->
      ?assertEqual(true, false) %TODO not proud
  end,
  ?assertEqual(1, erldis:publish(Pub, <<"multibidule">>, <<"baz">>)),
  receive
    {message, <<"multibidule">>, M2} ->
      ?assertEqual(<<"baz">>, M2)
  after
    100 ->
      ?assertEqual(true, false) %TODO not proud
  end.