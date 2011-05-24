-module(erldis_pool_sup_tests).

-include_lib("eunit/include/eunit.hrl").

pool_test() ->
  % kill the pool if it already exists
  erldis_pool_sup:stop(),
    
  ConnList = [
    {{"localhost", 6379}, 5},
    {{"localhost", 6379}, 2}
  ],
  unlink(element(2, erldis_pool_sup:start_link(ConnList))),
  Pids = erldis_pool_sup:get_pids({"localhost", 6379}),
  ?assertEqual(7, length(Pids)),
  
  RandomPid = erldis_pool_sup:get_random_pid({"localhost", 6379}),
  ?assertEqual([RandomPid], lists:filter(fun(Pid) -> Pid =:= RandomPid end, Pids)),
  
  % Increment a key on each connection
  PoolSupCounterKey = <<"pool_sup_counter">>,
  lists:foreach(fun(Client) ->
    erldis:incr(Client, PoolSupCounterKey)
  end, erldis_pool_sup:get_pids({"localhost", 6379})),
  
  % Check the counter
  ?assertEqual(integer_to_list(length(Pids)), binary_to_list(erldis:get(lists:nth(1, Pids), PoolSupCounterKey))).