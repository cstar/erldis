-module(erldis_sup).

-behaviour(supervisor).

-export([start_link/0, client/0, init/1]).

start_link() ->
	{ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
	{ok, Pid, client()}.

client() ->
	% Client pid() is undefined if not started
	[{erldis_client, Client, worker, [erldis_client]}] = supervisor:which_children(?MODULE),
	% not trying to restart_child(client) because gen_tcp:connect will hang
	% even if timeout is given
	Client.

init(_Args) ->
	{ok, {{one_for_one, 1, 60}, [
		% transient restart so client can disconnect safely
		% timeout so client has time to disconnect on exit
		{erldis_client, {erldis_client, start_link, []},
		 transient, 500, worker, [erldis_client]}
	]}}.