-module(erldis_list_tests).

-include_lib("eunit/include/eunit.hrl").

queue_test() ->
	Client = setup(),
	% queue api
	?assertEqual(true, erldis_list:is_empty(<<"foo">>, Client)),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	?assertEqual(empty, erldis_list:out(<<"foo">>, Client)),
	erldis_list:in(<<"a">>, <<"foo">>, Client),
	?assertEqual(false, erldis_list:is_empty(<<"foo">>, Client)),
	erldis_list:in(<<"b">>, <<"foo">>, Client),
	?assertEqual(2, erldis_list:len(<<"foo">>, Client)),
	?assertEqual({value, <<"a">>}, erldis_list:out(<<"foo">>, Client)),
	?assertEqual(1, erldis_list:len(<<"foo">>, Client)),
	erldis_list:in_r(<<"x">>, <<"foo">>, Client),
	?assertEqual({value, <<"b">>}, erldis_list:out_r(<<"foo">>, Client)),
	?assertEqual(false, erldis_list:is_empty(<<"foo">>, Client)),
	?assertEqual({value, <<"x">>}, erldis_list:out(<<"foo">>, Client)),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	?assertEqual(empty, erldis_list:out(<<"foo">>, Client)),
	erldis_client:stop(Client).

extended_queue_test() ->
	Client = setup(),
	?assertEqual(empty, erldis_list:get(<<"foo">>, Client)),
	?assertEqual(empty, erldis_list:get_r(<<"foo">>, Client)),
	?assertEqual(empty, erldis_list:peek(<<"foo">>, Client)),
	?assertEqual(empty, erldis_list:peek_r(<<"foo">>, Client)),
	erldis_list:in(<<"a">>, <<"foo">>, Client),
	erldis_list:in(<<"b">>, <<"foo">>, Client),
	?assertEqual(<<"a">>, erldis_list:get(<<"foo">>, Client)),
	?assertEqual(<<"b">>, erldis_list:get_r(<<"foo">>, Client)),
	?assertEqual(2, erldis_list:len(<<"foo">>, Client)),
	?assertEqual({value, <<"a">>}, erldis_list:peek(<<"foo">>, Client)),
	?assertEqual({value, <<"b">>}, erldis_list:peek_r(<<"foo">>, Client)),
	erldis_list:drop(<<"foo">>, Client),
	erldis_list:drop_r(<<"foo">>, Client),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	erldis_client:stop(Client).

array_test() ->
	Client = setup(),
	erldis_list:in(<<"a">>, <<"foo">>, Client),
	erldis_list:in(<<"b">>, <<"foo">>, Client),
	?assertEqual(<<"b">>, erldis_list:get(1, <<"foo">>, Client)),
	erldis_list:set(1, <<"x">>, <<"foo">>, Client),
	?assertEqual(<<"x">>, erldis_list:get(1, <<"foo">>, Client)),
	?assertEqual(2, erldis_list:size(<<"foo">>, Client)),
	?assertEqual({value, <<"a">>}, erldis_list:out(<<"foo">>, Client)),
	?assertEqual({value, <<"x">>}, erldis_list:out(<<"foo">>, Client)),
	erldis_client:stop(Client).

lists_test() ->
	Client = setup(),
	?assertEqual(false, erldis_list:is_list(<<"foo">>, Client)),
	?assertEqual([], erldis_list:sublist(<<"foo">>, Client, 1)),
	erldis_list:in(<<"a">>, <<"foo">>, Client),
	erldis_list:in(<<"b">>, <<"foo">>, Client),
	erldis_list:in(<<"c">>, <<"foo">>, Client),
	erldis_list:in(<<"b">>, <<"foo">>, Client),
	?assertEqual([<<"b">>, <<"c">>], erldis_list:sublist(<<"foo">>, Client, 2, 2)),
	?assertEqual(<<"b">>, erldis_list:nth(1, <<"foo">>, Client)),
	erldis_list:delete(<<"b">>, <<"foo">>, Client),
	?assertEqual(<<"c">>, erldis_list:nth(1, <<"foo">>, Client)),
	?assertEqual(3, erldis_list:len(<<"foo">>, Client)),
	?assertEqual([<<"c">>, <<"b">>], erldis_list:sublist(<<"foo">>, Client, 2, 2)),
	erldis_list:drop(<<"foo">>, Client),
	erldis_list:drop(<<"foo">>, Client),
	erldis_list:drop(<<"foo">>, Client),
	erldis_client:stop(Client).
	% this last call always produces a timeout error
	%?assertEqual([], erldis_list:sublist(<<"foo">>, Client, 3)).
	% TODO: test negative sublist start index

blocking_queue_test() ->
    Client = setup(),

    erldis:rpush(Client, <<"a">>, <<"value">>),
    ?assertEqual([<<"a">>, <<"value">>], erldis:blpop(Client, [<<"a">>, <<"b">>])),
    erldis:rpush(Client, <<"b">>, <<"value">>),
    ?assertEqual([<<"b">>, <<"value">>], erldis:blpop(Client, [<<"a">>, <<"b">>])),
    erldis:rpush(Client, <<"a">>, <<"first">>),
    erldis:rpush(Client, <<"a">>, <<"second">>),
    ?assertEqual([<<"a">>, <<"first">>], erldis:blpop(Client, [<<"a">>, <<"b">>])),
    ?assertEqual([<<"a">>, <<"second">>], erldis:blpop(Client, [<<"a">>, <<"b">>])),

    spawn_link(fun blocking_queue_sender/0),
    ?assertEqual([<<"a">>, <<1>>], erldis:blpop(Client, [<<"a">>, <<"b">>], 1000)),
    ?assertEqual([<<"b">>, <<1>>], erldis:blpop(Client, [<<"a">>, <<"b">>], 1000)),
    ?assertEqual([<<"a">>, <<2>>], erldis:blpop(Client, [<<"a">>, <<"b">>], 1000)),
    ?assertEqual([], erldis:blpop(Client, [<<"a">>, <<"b">>], 1000)),
    ?assertEqual([<<"a">>, <<3>>], erldis:blpop(Client, [<<"a">>, <<"b">>], 1000)),

    erldis_client:stop(Client).

blocking_queue_sender() ->
    Client = setup(),
    erldis:rpush(Client, <<"a">>, <<1>>),
    timer:sleep(100),
    erldis:rpush(Client, <<"b">>, <<1>>),
    timer:sleep(100),
    erldis:rpush(Client, <<"a">>, <<2>>),
    timer:sleep(3000),
    erldis:rpush(Client, <<"a">>, <<3>>),
    erldis_client:stop(Client).

foreach_test() ->
	Client = setup(),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	L = [<<"a">>, <<"b">>, <<"c">>],
	erldis_list:from_list(L, <<"foo">>, Client),
	?assertEqual(length(L), erldis_list:len(<<"foo">>, Client)),
	put(n, 1),
	
	F = fun(Item) ->
			N = get(n),
			?assertEqual(lists:nth(N, L), Item),
			put(n, N+1)
		end,
	
	erldis_list:foreach(F, <<"foo">>, Client),
	erldis_client:stop(Client).

merge_test() ->
	Client = setup(),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	L1 = [<<"a">>, <<"c">>, <<"e">>],
	erldis_list:from_list(L1, <<"foo">>, Client),
	?assertEqual(length(L1), erldis_list:len(<<"foo">>, Client)),
	L2 = [<<"b">>, <<"d">>, <<"f">>],
	F = fun(A, B) -> A =< B end,
	erldis_list:merge(F, L2, <<"foo">>, Client),
	Merged = lists:merge(F, L1, L2),
	?assertEqual(Merged, lists:merge(L1, L2)),
	?assertEqual(length(Merged), erldis_list:len(<<"foo">>, Client)),
	?assertEqual(Merged, erldis_list:to_list(<<"foo">>, Client)),
	
	L3 = [<<"a">>, <<"c">>, <<"f">>, <<"g">>],
	erldis_list:umerge(F, L3, <<"foo">>, Client),
	Merged2 = lists:umerge(F, Merged, L3),
	?assertEqual(Merged2, lists:umerge(Merged, L3)),
	?assertEqual(length(Merged2), erldis_list:len(<<"foo">>, Client)),
	?assertEqual(Merged2, erldis_list:to_list(<<"foo">>, Client)),
	
	erldis_client:stop(Client).

umerge_test() ->
	Client = setup(),
	Key = <<"foo">>,
	F = fun(A, B) -> A =< B end,
	?assertEqual(0, erldis_list:len(Key, Client)),
	L1 = [<<"a">>, <<"c">>, <<"e">>],
	erldis_list:umerge(F, L1, Key, Client),
	L1 = erldis_list:to_list(Key, Client),
	erldis_list:umerge(F, L1, Key, Client),
	L1 = erldis_list:to_list(Key, Client),
	erldis_list:umerge(F, [<<"a">>], Key, Client),
	L1 = erldis_list:to_list(Key, Client),
	erldis_client:stop(Client).

common_test() ->
	Client = setup(),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	L = [<<"a">>, <<"b">>, <<"c">>],
	erldis_list:from_list(L, <<"foo">>, Client),
	?assertEqual(length(L), erldis_list:len(<<"foo">>, Client)),
	% to_list uses foldr
	?assertEqual(L, erldis_list:to_list(<<"foo">>, Client)),
	% reverse uses foldl
	?assertEqual(lists:reverse(L), erldis_list:reverse(<<"foo">>, Client)),
	% from_list overwrites current list if it exists
	L2 = [<<"d">> | L],
	erldis_list:from_list(L2, <<"foo">>, Client),
	?assertEqual(length(L2), erldis_list:len(<<"foo">>, Client)),
	?assertEqual(L2, erldis_list:to_list(<<"foo">>, Client)),
	erldis_client:stop(Client).

extra_queue_test() ->
	Client = setup(),
	L = [<<"a">>, <<"b">>, <<"c">>],
	Length = length(L),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	erldis_list:from_list(L, <<"foo">>, Client),
	?assertEqual(Length, erldis_list:len(<<"foo">>, Client)),
	
	F = fun(Item) ->
			N = Length - erldis_list:len(<<"foo">>, Client),
			?assertEqual(lists:nth(N, L), Item)
		end,
	
	erldis_list:out_foreach(F, <<"foo">>, Client),
	?assertEqual(0, erldis_list:len(<<"foo">>, Client)),
	erldis_client:stop(Client).

setup() ->
	% setup
	application:load(erldis),
	{ok, Client} = erldis_client:connect(),
	?assertEqual(erldis:flushdb(Client), ok),
	Client.
