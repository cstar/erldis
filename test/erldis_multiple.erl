-module(erldis_multiple).
-export([test/0, get_key/1, server/2]).

test() ->
  {ok, Conn} = erldis:connect("localhost", 6379),
  erldis:zadd(Conn, "erldis_fail", 4.0, "thing1"),
  erldis:zadd(Conn, "erldis_fail", 4.0, "thing2"),
  erldis:zadd(Conn, "erldis_fail", 3.5, "thing3"),
  erldis:zadd(Conn, "erldis_fail", 2.0, "thing4"),
  erldis:zadd(Conn, "erldis_fail", 6.3, "thing5"),
  io:format("Expected Result: ~n"),
  io:format("~w~n", [erldis:zrevrange(Conn, "erldis_fail", 0, 30)]),
  io:format("SNIP ~n~n"),
  Pid = spawn_link(erldis_multiple, server, [self(), Conn]),
  run_test(Conn, 1000).

get_key(Conn) -> 
  %erldis:zrevrange_withscores(Conn, "erldis_fail", 0, 30).
  Res = send(Conn, fun(Redis)->
      erldis:zrevrange(Conn, "erldis_fail", 0, 30)
  end),
  try 5 = length(Res)
  catch
    error:{badmatch, _} -> 
      io:format("~w~n~n", [Res]),
      erlang:error(unexpected_result)
  end.

run_test(_Conn, 0) ->
  receive done -> done end;
  %done;
run_test(Conn, N) ->
  spawn_link(erldis_multiple, get_key, [Conn]),
  run_test(Conn, N-1).
  
send(Pid, Fun)->
  Pid ! {self(), Fun},
  receive {redis, Res} ->
    Res
  end.
  
server(Laucher, Conn) ->
  receive {Pid, Fun}->
    Pid ! {redis, Fun(Conn)},
    server(Laucher,Conn)
  after 1000 ->
    Laucher ! done
  end.
  