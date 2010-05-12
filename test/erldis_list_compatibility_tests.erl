-module(erldis_list_compatibility_tests).

-include_lib("eunit/include/eunit.hrl").

getset_test() ->
	Client = setup(),
	?assertEqual(erldis:exists(Client, <<"foo">>), false),
	?assertEqual(erldis:exists(Client, "foo"), false),
	?assertEqual(erldis:get(Client, <<"foo">>), nil),
	?assertEqual(erldis:get(Client, "foo"), nil),
	?assertEqual(erldis:set(Client, <<"foo">>, <<"bar">>), ok),
	?assertEqual(erldis:get(Client, <<"foo">>), <<"bar">>),
	?assertEqual(erldis:get(Client, "foo"), <<"bar">>),
	?assertEqual(erldis:exists(Client, <<"foo">>), true),
	?assertEqual(erldis:exists(Client, "foo"), true),
	?assertEqual(erldis:del(Client, "foo"), true),
	?assertEqual(erldis:exists(Client, <<"foo">>), false),
	?assertEqual(erldis:exists(Client, "foo"), false),
	?assertEqual(erldis:set(Client, "fool", "baz"), ok),
	?assertEqual(erldis:get(Client, "fool"), <<"baz">>),
	?assertEqual(erldis:get(Client, <<"fool">>), <<"baz">>),
	?assertEqual(erldis:exists(Client, <<"fool">>), true),
	?assertEqual(erldis:exists(Client, "fool"), true),
	?assertEqual(erldis:del(Client, <<"fool">>), true),
	?assertEqual(erldis:exists(Client, <<"fool">>), false),
	?assertEqual(erldis:exists(Client, "fool"), false).

incrdecr_test() ->
	Client = setup(),
	?assertEqual(erldis:incr(Client, <<"foo">>), 1),
	?assertEqual(erldis:incr(Client, "foo"), 2),
	?assertEqual(erldis:incrby(Client, <<"foo">>, 2), 4),
	?assertEqual(erldis:incrby(Client, "foo", 3), 7),
	?assertEqual(erldis:decr(Client, <<"foo">>), 6),
	?assertEqual(erldis:decr(Client, "foo"), 5),
	?assertEqual(erldis:decrby(Client, <<"foo">>, 2), 3),
	?assertEqual(erldis:decrby(Client, "foo", 3), 0).

list_test() ->
	Client = setup(),
	?assertEqual(erldis:exists(Client, "foo"), false),
	?assertEqual(erldis:lpush(Client, "foo", "a"), 1),
	?assertEqual(erldis:rpush(Client, "foo", "b"), 2),
	?assertEqual(erldis:lindex(Client, "foo", 0), <<"a">>),
	?assertEqual(erldis:lindex(Client, "foo", 1), <<"b">>),
	?assertEqual(erldis:llen(Client, "foo"), 2),
	?assertEqual(erldis_list:is_list("foo", Client), true).

set_test() ->
	Client = setup(),
	?assertEqual(erldis:exists(Client, "foo"), false),
	?assertEqual(erldis:sadd(Client, "foo", "bar"), true),
	?assertEqual(erldis:scard(Client, "foo"), 1),
	?assertEqual(erldis:smembers(Client, "foo"), [<<"bar">>]),
	?assertEqual(erldis:sismember(Client, "foo", "bar"), true),
	?assertEqual(erldis_sets:is_set(Client, "foo"), true).

json_test() ->
	% requires yaws json module
	Arr = {array, ["a", "b", "c"]},
        case code:which(json) of
          non_existing -> ok;
          _ -> Json = json:encode(Arr),
          	Client = setup(),
        	?assertEqual(erldis:set(Client, "json", Json), ok),
        	GetJson = erldis:get(Client, "json"),
        	?assertEqual(GetJson, list_to_binary(Json)),
        	{ok, Arr2} = json:decode_string(binary_to_list(GetJson)),
        	?assertEqual(Arr, Arr2)
        end.

setup() ->
	% setup
	application:load(erldis),
	{ok, Client} = erldis_client:connect(),
	?assertEqual(erldis:flushdb(Client), ok),
	Client.
