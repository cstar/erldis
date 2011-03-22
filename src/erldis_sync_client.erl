%% @doc This is a gen_server very similar to erldis_client, but it does
%% synchronous calls instead of async pipelining. Does so by keeping a queue
%% of From pids in State.calls, then calls gen_server:reply when it receives
%% handle_info({tcp, ...). Therefore, it must get commands on handle_call
%% instead of handle_cast, which requires direct command sending instead of
%% using the API in erldis.
%%
%% @todo Much of the code has been copied from erldis_client and should be
%% abstracted & shared where possible
%%
%% @author Jacob Perkins <japerk@gmail.com>
-module(erldis_sync_client).

-behaviour(gen_server).

-include("erldis.hrl").

-export([scall/2, scall/3, call/2, call/3, stop/1, transact/1, transact/2, select/2, info/1,  sr_scall/2, sr_scall/3]).
-export([connect/0, connect/1, connect/2, connect/3, connect/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2, code_change/3]).

%%%%%%%%%%%%%
%% helpers %%
%%%%%%%%%%%%%

% TODO: copied from erldis_client, should be abstracted & shared

str(X) when is_list(X) ->
	X;
str(X) when is_atom(X) ->
	atom_to_list(X);
str(X) when is_binary(X) ->
	binary_to_list(X);
str(X) when is_integer(X) ->
	integer_to_list(X);
str(X) when is_float(X) ->
	float_to_list(X).

format([], Result) ->
	string:join(lists:reverse(Result), ?EOL);
format([Line|Rest], Result) ->
	JoinedLine = string:join([str(X) || X <- Line], " "),
	format(Rest, [JoinedLine|Result]).

format(Lines) ->
	format(Lines, []).
sformat(Line) ->
	format([Line], []).

trim2({ok, S}) ->
	string:substr(S, 1, length(S)-2);
trim2(S) ->
	trim2({ok, S}).

app_get_env(AppName, Varname, Default) ->
	case application:get_env(AppName, Varname) of
		undefined ->
			{ok, Default};
		V ->
			V
	end.

ensure_started(#redis{socket=undefined, host=Host, port=Port, timeout=Timeout}=State)->
	Opts = [list, {active, once}, {packet, line}, {nodelay, true}, {send_timeout, Timeout}],
	
	case gen_tcp:connect(Host, Port, Opts, Timeout) of
		{ok, Socket} ->
			error_logger:info_report([{?MODULE, reconnected}, State]),
			State#redis{socket=Socket};
		{error, Why} ->
			Report = [{?MODULE, unable_to_connect}, {error, Why}, State],
			error_logger:warning_report(Report),
			State
	end;
ensure_started(State)->
	State.

%%%%%%%%%%%%%%%%%%
%% call command %%
%%%%%%%%%%%%%%%%%%
sr_scall(Client, Cmd) -> sr_scall(Client, Cmd, []).
sr_scall(Client, Cmd, Args) -> 
  [R] = scall(Client, Cmd, Args),
  R.

% This is the simple send with a single row of commands
scall(Client, Cmd) -> scall(Client, Cmd, []).

scall(Client, Cmd, Args) ->
	case gen_server:call(Client, {send, sformat([Cmd|Args])}) of
		{error, Reason} -> throw({error, Reason});
		Retval -> Retval
	end.

% This is the complete send with multiple rows
call(Client, Cmd) -> call(Client, Cmd, []).

call(Client, Cmd, Args) ->
	SCmd = string:join([str(Cmd), format(Args)], " "),
	
	case gen_server:call(Client, {send, SCmd}) of
		{error, Reason} -> throw({error, Reason});
		Retval -> Retval
	end.

% stop is synchronous so can be sure that client is shutdown
stop(Client) -> gen_server:call(Client, disconnect).

transact(F) ->
	case connect() of
		{error, Error} -> {error, Error};
		{ok, Client} -> transact(Client, F)
	end.

transact(DB, F) when is_integer(DB) ->
	case connect(DB) of
		{error, Error} -> {error, Error};
		{ok, Client} -> transact(Client, F)
	end;
transact(Client, F) when is_pid(Client) ->
	try F(Client) of
		Result -> stop(Client), Result
	catch
		throw:Result -> stop(Client), throw(Result);
		error:Result -> stop(Client), {error, Result};
		exit:Result -> stop(Client), exit(Result)
	end.

select(Client, DB) ->
	[ok] = scall(Client, select, [DB]),
	Client.

info(Client) ->
	F = fun(Stat) ->
			case parse_stat(Stat) of
				undefined -> false;
				{Key, Val} -> {Key, Val}
			end
		end,
	
	[S] = scall(Client, info),
	elists:mapfilter(F, string:tokens(S, "\r\n")).

parse_stat("redis_version:"++Vsn) ->
	{version, Vsn};
parse_stat("uptime_in_seconds:"++Val) ->
	{uptime, list_to_integer(Val)};
parse_stat("connected_clients:"++Val) ->
	{clients, list_to_integer(Val)};
parse_stat("connected_slaves:"++Val) ->
	{slaves, list_to_integer(Val)};
parse_stat("used_memory:"++Val) ->
	{memory, list_to_integer(Val)};
parse_stat("changes_since_last_save:"++Val) ->
	{changes, list_to_integer(Val)};
parse_stat("last_save_time:"++Val) ->
	{last_save, list_to_integer(Val)};
parse_stat("total_connections_received:"++Val) ->
	{connections, list_to_integer(Val)};
parse_stat("total_commands_processed:"++Val) ->
	{commands, list_to_integer(Val)};
parse_stat(_) ->
	undefined.

%%%%%%%%%%
%% init %%
%%%%%%%%%%

connect() ->
	{ok, Host} = app_get_env(erldis, host, "localhost"),
	connect(Host).

connect(Host) when is_list(Host) ->
	{ok, Port} = app_get_env(erldis, port, 6379),
	connect(Host, Port);
connect(DB) when is_integer(DB) ->
	case connect() of
		{ok, Client} -> {ok, select(Client, DB)};
		Other -> Other
	end.

connect(Host, Port) ->
	{ok, Timeout} = app_get_env(erldis, timeout, 500),
	connect(Host, Port, [{timeout, Timeout}]).

connect(Host, Port, Options) ->
	% not using start_link because caller may not want to crash if this
	% server is shutdown
	gen_server:start(?MODULE, [Host, Port], Options).

connect(Host, Port, Options, DB) ->
	case connect(Host, Port, Options) of
		{ok, Client} -> {ok, select(Client, DB)};
		Other -> Other
	end.

init([Host, Port]) ->
	process_flag(trap_exit, true),
	{ok, Timeout} = app_get_env(erldis, timeout, 500),
	% presence of send_timeout_close Opt causes {error, badarg}
	Opts = [list, {active, once}, {packet, line}, {nodelay, true},
			{send_timeout, Timeout}],
	% without timeout, default is infinity
	case gen_tcp:connect(Host, Port, Opts, Timeout) of
		{error, Why} ->
			{stop, {socket_error, Why}};
		{ok, Socket} ->
			% calls is a queue instead of a count
			{ok, #redis{socket=Socket, calls=queue:new(), host=Host, port=Port, timeout=Timeout}}
	end.

%%%%%%%%%%%%%%%%%
%% handle_call %%
%%%%%%%%%%%%%%%%%

handle_call({send, Cmd}, From, State1) ->
	% NOTE: redis ignores sent commands it doesn't understand, which means
	% we don't get a reply, which means callers will timeout
	State = ensure_started(State1),
	case gen_tcp:send(State#redis.socket, [Cmd|?EOL]) of
		ok ->
			%error_logger:info_report([{send, Cmd}, {from, From}]),
			Queue = queue:in(From, State#redis.calls),
			{noreply, State#redis{calls=Queue, remaining=1}};
		{error, Reason} ->
			error_logger:error_report([{send, Cmd}, {error, Reason}]),
			{stop, timeout, {error, Reason}, State}
	end;
handle_call(disconnect, _, State) ->
	{stop, shutdown, shutdown, State};
handle_call(_, _, State) ->
	{reply, undefined, State}.

handle_cast(disconnect, State) ->
	{stop, shutdown, State};
handle_cast(_, State) ->
	{noreply, State}.

%%%%%%%%%%%%%%%%%
%% handle_info %%
%%%%%%%%%%%%%%%%%

recv_value(Socket, NBytes) ->
	inet:setopts(Socket, [{packet, 0}]), % go into raw mode to read bytes
	
	case gen_tcp:recv(Socket, NBytes+2) of
		{ok, Packet} ->
			inet:setopts(Socket, [{packet, line}]), % go back to line mode
			trim2({ok, Packet});
		{error, Reason} ->
			error_logger:error_report([{recv, NBytes}, {error, Reason}]),
			throw({error, Reason})
	end.

send_reply(State) ->
	{{value, From}, Queue} = queue:out(State#redis.calls),
	Reply = lists:reverse(State#redis.buffer),
	%error_logger:info_report([{reply, Reply}, {to, From}]),
	gen_server:reply(From, Reply),
	State#redis{calls=Queue, buffer=[], pstate=empty}.

parse_state(State, Socket, Data) ->
	Parse = erldis_proto:parse(State#redis.pstate, trim2(Data)),
	case {State#redis.remaining-1, Parse} of
		{0, error} ->
			% next line is the error string
			State#redis{remaining=1, pstate=error};
		{0, {hold, nil}} ->
			% reply with values in buffer
			send_reply(State);
		{0, {hold, Remaining}} ->
			% begin accumulation of multi bulk reply
			State#redis{remaining=Remaining, pstate=read};
		{_, {read, nil}} ->
			% reply with nil
			send_reply(State#redis{buffer=[nil]});
	  {_, {read, 0}} ->
			% reply with nil
			send_reply(State#redis{buffer=[]});
		{0, {read, NBytes}} ->
			% reply with Value added to buffer
			Value = recv_value(Socket, NBytes),
			Buffer = [Value | State#redis.buffer],
			send_reply(State#redis{buffer=Buffer});
		{N, {read, NBytes}} ->
			% accumulate multi bulk reply
			Value = recv_value(Socket, NBytes),
			Buffer = [Value | State#redis.buffer],
			State#redis{remaining=N, buffer=Buffer, pstate=read};
		{0, Value} ->
			% reply with Value
			send_reply(State#redis{buffer=[Value]})
	end.

handle_info({tcp, Socket, Data}, State) ->
	case (catch parse_state(State, Socket, Data)) of
		{error, Reason} ->
			error_logger:error_report([{parse_state, Data}, {error, Reason}]),
			{stop, Reason, State};
		NewState ->
			inet:setopts(Socket, [{active, once}]),
			{noreply, NewState}
	end;
handle_info({tcp_closed, Socket}, State=#redis{socket=Socket}) ->
	error_logger:warning_report([{erldis_sync_client, tcp_closed}, State]),
    {noreply, State#redis{socket=undefined}};
handle_info(_Info, State) ->
	{noreply, State}.

%%%%%%%%%%%%%%%
%% terminate %%
%%%%%%%%%%%%%%%

terminate(_Reason, State) ->
	% NOTE: if supervised with brutal_kill, may not be able to reply
	R = fun(From) -> gen_server:reply(From, {error, closed}) end,
	lists:foreach(R, queue:to_list(State#redis.calls)),
	
	case State#redis.socket of
		undefined -> ok;
		Socket -> gen_tcp:close(Socket)
	end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
