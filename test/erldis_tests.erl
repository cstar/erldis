-module(erldis_tests).

-include_lib("eunit/include/eunit.hrl").
-include("erldis.hrl").

%quit_test() ->
%	 {ok, Client} = erldis:connect(<<"localhost">>, 6379),
%	 ok = erldis:quit(Client),
%	 false = is_process_alive(Client).
%
utils_test() ->
	?assertEqual(erldis_client:bin(1), <<"1">>),
	?assertEqual(erldis_client:bin(atom), <<"atom">>),
	?assertEqual(erldis_client:format([[1, 2, 3]]), <<"1 2 3">>),
	?assertEqual(erldis_client:format([[1,2,3], [4,5,6]]), <<"1 2 3\r\n4 5 6">>).

basic_test() ->
	{ok, Client} = erldis:connect("localhost", 6379),
	erldis:flushall(Client),

	nil = erldis:get(Client, <<"pippo">>),
	ok = erldis:set(Client, <<"hello">>, <<"kitty!">>),
	true = erldis:setnx(Client, <<"foo">>, <<"bar">>),
	false = erldis:setnx(Client, <<"foo">>, <<"bar">>),

	true = erldis:exists(Client, <<"hello">>),
	true = erldis:exists(Client, <<"foo">>),
	<<"bar">> = erldis:get(Client, <<"foo">>),
	[<<"kitty!">>, <<"bar">>] = erldis:mget(Client, [<<"hello">>, <<"foo">>]),
	true = erldis:del(Client, <<"hello">>),
	true = erldis:del(Client, <<"foo">>),
	false = erldis:exists(Client, <<"hello">>),
	false = erldis:exists(Client, <<"foo">>),

	erldis:sadd(Client, <<"set">>, <<"toto">>),
	[<<"toto">>] = erldis:smembers(Client, <<"set">>),
	erldis:srem(Client, <<"set">>, <<"toto">>),
	[] = erldis:smembers(Client, <<"set">>).

	%%% Commented out. Using the new erldis_set, erldis_list.
	%ok = erldis:set(Client, <<"pippo">>, <<"pluto">>),
	%{error, <<"ERR Operation against a key holding the wrong kind of value"} = erldis:sadd(Client, <<"pippo">>, <<"paperino">>),
	%% foo doesn't exist, the result will be nil
	%[] = erldis:lrange(Client, <<"foo">>, 1, 2),
	%true = erldis:del(Client, <<"pippo">>),
	%
	%ok = erldis:rpush(Client, <<"a_list">>, <<"1">>),
	%ok = erldis:rpush(Client, <<"a_list">>, <<"2">>),
	%ok = erldis:rpush(Client, <<"a_list">>, <<"3">>),
	%%ok = erldis:rpush(Client, <<"a_list">>, <<"1">>),
	%%true = erldis:lrem(Client, <<"a_list">>, 1, <<"1">>).
	%[<<"2">>, <<"3">>, <<"1">>] = erldis:lrange(Client, <<"a_list">>, 0, 2),
	%
	%[<<"1">>, <<"2">>, <<"3">>] = erldis:sort(Client, <<"a_list">>),
	%[<<"3">>, <<"2">>, <<"1">>] = erldis:sort(Client, <<"a_list">>, <<"DESC">>),
	%[<<"2">>, <<"3">>, <<"1">>] = erldis:lrange(Client, <<"a_list">>, 0, 2),
	%[<<"1">>, <<"2">>] = erldis:sort(Client, <<"a_list">>, <<"LIMIT 0 2 ASC">>).
	%ok = erldis:quit(Client).



% inline_tests(Client) ->
%	  [?_assertMatch(ok, erldis:set(Client, <<"hello">>, <<"kitty!">>)),
%	   ?_assertMatch(false, erldis:setnx(Client, <<"hello">>, <<"kitty!">>)),
%	   ?_assertMatch(true, erldis:exists(Client, <<"hello">>)),
%	   ?_assertMatch(true, erldis:del(Client, <<"hello">>)),
%	   ?_assertMatch(false, erldis:exists(Client, <<"hello">>)),
%
%	   ?_assertMatch(true, erldis:setnx(Client, <<"hello">>, <<"kitty!">>)),
%	   ?_assertMatch(true, erldis:exists(Client, <<"hello">>)),
%	   ?_assertMatch("kitty!">>, erldis:get(Client, <<"hello">>)),
%	   ?_assertMatch(true, erldis:del(Client, <<"hello">>)),
%
%
%	   ?_assertMatch(1, erldis:incr(Client, <<"pippo">>))
%	   ,?_assertMatch(2, erldis:incr(Client, <<"pippo">>))
%	   ,?_assertMatch(1, erldis:decr(Client, <<"pippo">>))
%	   ,?_assertMatch(0, erldis:decr(Client, <<"pippo">>))
%	   ,?_assertMatch(-1, erldis:decr(Client, <<"pippo">>))
%
%	   ,?_assertMatch(6, erldis:incrby(Client, <<"pippo">>, 7))
%	   ,?_assertMatch(2, erldis:decrby(Client, <<"pippo">>, 4))
%	   ,?_assertMatch(-2, erldis:decrby(Client, <<"pippo">>, 4))
%	   ,?_assertMatch(true, erldis:del(Client, <<"pippo">>))
%	  ].
