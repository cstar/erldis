% japerk: do an erlang application with supervisor for client so that there
% is only one connection. Otherwise was getting issues with gen_tcp hanging
% on connect, even with a Timeout given.
-module(erldis_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) -> erldis_sup:start_link().

stop(Client) when is_pid(Client) ->
	erldis:quit(Client);
stop(_State) ->
	case erldis_sup:client() of
		undefined -> ok;
		Client -> erldis:quit(Client)
	end.