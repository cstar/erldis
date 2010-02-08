-module(erldis_sets_tests).

-include_lib("eunit/include/eunit.hrl").

sets_test() ->
	% setup
	application:load(erldis),
	{ok, Client} = erldis_client:connect(),
	?assertEqual(erldis:flushdb(Client), ok),
	% non existent set
	?assertEqual(erldis_sets:is_set(Client, <<"foo">>), false),
	?assertEqual(erldis_sets:to_list(Client, <<"foo">>), []),
	?assertEqual(erldis_sets:size(Client, <<"foo">>), 0),
	?assertEqual(erldis_sets:is_element(<<"bar">>, Client, <<"foo">>), false),
	% add 1 element, values must be strings/lists, can't be integers
	erldis_sets:add_element(<<"1">>, Client, <<"foo">>),
	?assertEqual(erldis_sets:is_set(Client, <<"foo">>), true),
	?assertEqual(erldis_sets:size(Client, <<"foo">>), 1),
	?assertEqual(erldis_sets:is_element(<<"1">>, Client, <<"foo">>), true),
	?assertEqual(erldis_sets:to_list(Client, <<"foo">>), [<<"1">>]),
	% add 2 element
	erldis_sets:add_element(<<"2">>, Client, <<"foo">>),
	?assertEqual(erldis_sets:size(Client, <<"foo">>), 2),
	?assertEqual(erldis_sets:is_element(<<"2">>, Client, <<"foo">>), true),
	?assertEqual(lists:sort(erldis_sets:to_list(Client, <<"foo">>)), [<<"1">>, <<"2">>]),
	% del 2 element
	erldis_sets:del_element(<<"2">>, Client, <<"foo">>),
	?assertEqual(erldis_sets:size(Client, <<"foo">>), 1),
	?assertEqual(erldis_sets:is_element(<<"2">>, Client, <<"foo">>), false),
	% from list
	Elems = [<<"a">>, <<"b">>, <<"c">>],
	erldis_sets:from_list(Client, <<"foo">>, Elems),
	?assertEqual(lists:sort(erldis_sets:to_list(Client, <<"foo">>)), Elems),
	erldis_client:stop(Client).
	% TODO: test union, intersection, is_disjoint, subtract.

combo_sets_test() ->
	application:load(erldis),
	{ok, Client} = erldis_client:connect(),
	?assertEqual(ok, erldis:flushdb(Client)),
	
	?assertEqual(true, erldis_sets:add_element(<<"1">>, Client, <<"foo">>)),
	?assertEqual(true, erldis_sets:add_element(<<"2">>, Client, <<"foo">>)),
	?assertEqual(true, erldis_sets:add_element(<<"3">>, Client, <<"foo">>)),
	
	?assertEqual(true, erldis_sets:add_element(<<"2">>, Client, <<"bar">>)),
	?assertEqual(true, erldis_sets:add_element(<<"3">>, Client, <<"bar">>)),
	?assertEqual(true, erldis_sets:add_element(<<"4">>, Client, <<"bar">>)),
	
	Union = erldis_sets:union(Client, [<<"foo">>, <<"bar">>]),
	?assertEqual([<<"1">>, <<"2">>, <<"3">>, <<"4">>], lists:sort(Union)),
	?assertEqual(4, erldis:sunionstore(Client, <<"union">>, [<<"foo">>, <<"bar">>])),
	?assertEqual(lists:sort(Union), lists:sort(erldis:smembers(Client, <<"union">>))),
	
	?assertEqual([<<"1">>], erldis_sets:subtract(Client, <<"foo">>, <<"bar">>)),
	?assertEqual([<<"4">>], erldis_sets:subtract(Client, <<"bar">>, <<"foo">>)),
	?assertEqual(1, erldis:sdiffstore(Client, <<"diff">>, [<<"foo">>, <<"bar">>])),
	?assertEqual([<<"1">>], erldis:smembers(Client, <<"diff">>)),
	
	Inter = erldis_sets:intersection(Client, [<<"foo">>, <<"bar">>]),
	?assertEqual([<<"2">>, <<"3">>], lists:sort(Inter)),
	?assertEqual(2, erldis:sinterstore(Client, <<"inter">>, [<<"foo">>, <<"bar">>])),
	?assertEqual(lists:sort(Inter), lists:sort(erldis:smembers(Client, <<"inter">>))).