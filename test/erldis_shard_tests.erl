-module(erldis_shard_tests).

-include_lib("eunit/include/eunit.hrl").

shard_test() ->
    % kill the pool if it already exists and wait
    erldis_pool_sup:stop(),
    
    ShardSpec = [
        {"redis01", [{{{"127.0.0.1", 6379}, 1}, master}]},
        {"redis02", [{{{"127.0.0.1", 6380}, 1}, master1}]},
        {"redis03", [{{{"127.0.0.1", 6380}, 1}, master2}]}
    ],
    unlink(element(2, erldis_shard:start_link(ShardSpec))),
    ?assertEqual(1, length(erldis_pool_sup:get_pids({"127.0.0.1", 6379}))),
    ?assertEqual(2, length(erldis_pool_sup:get_pids({"127.0.0.1", 6380}))),
    ?assertEqual(["redis01", "redis02", "redis03"], 
        [erldis_shard:get_slot("a"), erldis_shard:get_slot("b"), erldis_shard:get_slot("f")]),
    
    ?assertEqual(true, is_pid(erldis_shard:client("f", master2))),
    ?assertEqual(false, is_pid(erldis_shard:client("f", master))),
    
    ?assertEqual(true, is_pid(erldis_shard:client("b", master1))),
    ?assertEqual(false, is_pid(erldis_shard:client("b", master))),
    
    ?assertEqual(true, is_pid(erldis_shard:client("a", master))),
    ?assertEqual(false, is_pid(erldis_shard:client("a", master2))),
    
    % write something that should be on one shard, verify it is there, then try to read from a different
    % shard
    ShardTestKey = <<"a">>,
    erldis:del(erldis_shard:client("b", master1), "b"),
    erldis:del(erldis_shard:client("b", master1), "a"),
    erldis:set(erldis_shard:client(ShardTestKey, master), ShardTestKey, "valid"),
    ?assertEqual(<<"valid">>, erldis:get(erldis_shard:client(ShardTestKey, master), ShardTestKey)),
    ?assertEqual(nil, erldis:get(erldis_shard:client("b", master1), ShardTestKey)).