%% @doc This is a	 very similar to erldis_client, but it does
%% synchronous calls instead of async pipelining. Does so by keeping a queue
%% of From pids in State.calls, then calls gen_server2:reply when it receives
%% handle_info({tcp, ...). Therefore, it must get commands on handle_call
%% instead of handle_cast, which requires direct command sending instead of
%% using the API in erldis.
%%
%% @todo Much of the code has been copied from erldis_client and should be
%% abstracted & shared where possible
%%
%% @author Jacob Perkins <japerk@gmail.com>
-module(erldis_client).

-behaviour(gen_server2).

-include("erldis.hrl").

-export([sr_scall/2, scall/2, scall/3, call/2, call/3, bcall/3,
		 set_call/4, send/3]).
-export([stop/1, transact/1, transact/2, select/2]).
-export([connect/0, connect/1, connect/2, connect/3, connect/4]).
-export([start_link/0, start_link/1, start_link/2, start_link/3, start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
		 terminate/2, code_change/3]).
-export([subscribe/4, unsubscribe/3]).

-define(default_timeout, 5000). %% same as in gen.erl in stdlib

%%%%%%%%%%%%%
%% helpers %%
%%%%%%%%%%%%%

trim2({ok, S}) ->
	Read = size(S)-2,
	<<R:Read/bytes, _/binary>> = S,
	R;
trim2(S) ->
	trim2({ok, S}).

app_get_env(AppName, Varname, Default) ->
	case application:get_env(AppName, Varname) of
		undefined -> {ok, Default};
		V -> V
	end.

%%%%%%%%%%%%%%%%%%%
%% call commands %%
%%%%%%%%%%%%%%%%%%%

select(Client, DB) ->
	erldis:select(Client, DB),
	Client.

sr_scall(Client, Args) ->
	case scall(Client, Args) of
		[R] -> R;
		[] -> nil;
		ok -> ok
	end.

% This is the simple send with a single row of commands
scall(Client, Args) -> scall(Client, Args, ?default_timeout).

scall(Client, Args, Timeout) ->
	send(Client, erldis_proto:multibulk_cmd(Args), Timeout).

% This is the complete send with multiple rows
call(Client, Args) -> call(Client, Args, ?default_timeout).

call(Client, Args, Timeout) -> send(Client,
	erldis_proto:multibulk_cmd(Args), Timeout).

% Blocking call with server-side timeout added as final command arg
bcall(Client, Args, Timeout) ->
	scall(Client, Args ++ [server_timeout(Timeout)], erlang_timeout(Timeout)).

set_call(Client, Cmd, Key, Val) when is_binary(Val) ->
	call(Client, Cmd, [[Key, erlang:size(Val)], [Val]]);
set_call(Client, Cmd, Key, Val) ->
	set_call(Client, Cmd, Key, erldis_binaries:to_binary(Val)).

subscribe(Client, Cmd, Class, Pid)->
	case gen_server2:call(Client, {subscribe, Cmd, Class, Pid}, ?default_timeout) of
		{error, Reason} -> throw({error, Reason});
		Retval -> Retval
	end.

unsubscribe(Client, Cmd, Class)->
	case gen_server2:call(Client, {unsubscribe, Cmd, Class}, ?default_timeout) of
		{error, Reason} -> throw({error, Reason});
		Retval -> Retval
	end.

% Erlang uses milliseconds, with symbol "infinity" for "wait forever";
% redis uses seconds, with 0 for "wait forever".
server_timeout(infinity) -> 0;
server_timeout(V) when is_number(V) -> round(V / 1000).

% Kludge on a few milliseconds to the timeout we gave the server, to
% give the network and client a chance to catch up.
erlang_timeout(infinity) -> infinity;
erlang_timeout(V) when is_number(V) -> V + ?default_timeout.

send(Client, Cmd, Timeout) ->
	Piped = gen_server2:call(Client, is_pipelined),
	
	if
		Piped ->
			gen_server2:cast(Client, {send, Cmd});
		true ->
			case gen_server2:call(Client, {send, Cmd}, Timeout) of
				{error, Reason} -> throw({error, Reason});
				Retval -> Retval
			end
	end.

% TODO: use multi/exec for transact (and move transact to erldis.erl)
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

%%%%%%%%%%%%%%%%%%%%
%% connect & init %%
%%%%%%%%%%%%%%%%%%%%

connect() -> start(false).
connect(Host) when is_list(Host) -> start(Host, false);
connect(DB) when is_integer(DB) -> start(DB, false).
connect(Host, Port) -> start(Host, Port, false).
connect(Host, Port, Options) -> start(Host, Port, Options, false).
connect(Host, Port, Options, DB) -> start(Host, Port, Options, DB, false).

start_link() -> start(true).
start_link(Host) when is_list(Host) -> start(Host, true);
start_link(DB) when is_integer(DB) -> start(DB, true).
start_link(Host, Port) -> start(Host, Port, true).
start_link(Host, Port, Options) -> start(Host, Port, Options, true).
start_link(Host, Port, Options, DB) -> start(Host, Port, Options, DB, true).

start(ShouldLink) ->
	{ok, Host} = app_get_env(erldis, host, "localhost"),
	start(Host, ShouldLink).

start(Host, ShouldLink) when is_list(Host) ->
	{ok, Port} = app_get_env(erldis, port, 6379),
	start(Host, Port, ShouldLink);
start(DB, ShouldLink) when is_integer(DB) ->
	case start(ShouldLink) of
		{ok, Client} -> {ok, select(Client, DB)};
		Other -> Other
	end.

start(Host, Port, ShouldLink) ->
	{ok, Timeout} = app_get_env(erldis, timeout, 500),
	start(Host, Port, [{timeout, Timeout}], ShouldLink).

start(Host, Port, Options, false) ->
	% not using start_link because caller may not want to crash if this
	% server is shutdown
	gen_server2:start(?MODULE, [Host, Port, false], Options);
start(Host, Port, Options, true) ->
	gen_server2:start_link(?MODULE, [Host, Port, true], Options).

start(Host, Port, Options, DB, ShouldLink) ->
	case start(Host, Port, Options, ShouldLink) of
		{ok, Client} -> {ok, select(Client, DB)};
		Other -> Other
	end.

% stop is synchronous so can be sure that client is shutdown
stop(Client) -> gen_server2:call(Client, disconnect).

% ShouldRegister indicates if the client needs to be stores in the erldis_pool_sup ETS
init([Host, Port, _ShouldRegister]=Params) ->
	process_flag(trap_exit, true),
	{ok, Timeout} = app_get_env(erldis, timeout, 500),
	State = #redis{calls=queue:new(), host=Host, port=Port, timeout=Timeout, subscribers=dict:new()},
	
	% Add this Pid to the pool
	maybe_register(Params),
	
	case connect_socket(State, once) of
		{error, Why} -> {stop, {socket_error, Why}};
		{ok, NewState} -> {ok, NewState}
	end.

maybe_register([Host, Port, true])->
    erldis_pool_sup:add_pid({Host, Port}, self());
maybe_register([_Host, _Port, false])->
    ok.


ensure_started(#redis{socket=undefined, db=DB}=State) ->
	case connect_socket(State, false) of
		{error, Why} ->
			Report = [{?MODULE, unable_to_connect}, {error, Why}, State],
			error_logger:warning_report(Report),
			State;
		{ok, NewState} ->
			Socket = NewState#redis.socket,
			
			if
				DB == <<"0">> ->
					ok;
				true ->
					% send & recv here since don't have an active socket
					% because we want synchronous result since this is called
					% from handle_* functions
					Cmd = erldis_proto:multibulk_cmd([<<"select">>, DB]),
					gen_tcp:send(Socket, Cmd),
					{ok, <<"+OK", ?EOL>>} = gen_tcp:recv(Socket, 0)
			end,
			
			inet:setopts(Socket, [{active, once}]),
			NewState
	end;
ensure_started(State)->
	State.

connect_socket(#redis{socket=undefined, host=Host, port=Port, timeout=Timeout}=State, Active) ->
	% NOTE: send_timeout_close not used because causes {error, badarg}
	Opts = [binary, {active, Active}, {packet, line}, {nodelay, true},
			{send_timeout, Timeout}],
	% without timeout, default is infinity
	case gen_tcp:connect(Host, Port, Opts, Timeout) of
		{ok, Socket} -> {ok, State#redis{socket=Socket}};
		{error, Why} -> {error, Why}
	end;
connect_socket(State, _) ->
	{ok, State}.

%%%%%%%%%%%%%%%%%
%% handle_call %%
%%%%%%%%%%%%%%%%%

% Solves issue of remaining getting reset while still accumulating multi-bulk
% reply
dont_reset_remaining(State, Queue) ->
	case State#redis.remaining of
		0 -> State#redis{calls=Queue, remaining=1};
		_ -> State#redis{calls=Queue}
	end.

dont_reset_remaining(State, Queue, DB) ->
	case State#redis.remaining of
		0 -> State#redis{calls=Queue, remaining=1, db=DB};
		_ -> State#redis{calls=Queue, db=DB}
	end.

handle_call(is_pipelined, _From, State)->
	{reply, State#redis.pipeline, State};
handle_call(get_all_results, From, #redis{pipeline=true, calls=Calls}=State) ->
	case queue:len(Calls) of
		0 ->
			% answers came earlier than we could start listening...
			% Very unlikely but totally possible.
			Reply = lists:reverse(State#redis.results),
			{reply, Reply, State#redis{results=[], calls=Calls}};
		_ ->
			% We are here earlier than results came, so just make
			% ourselves wait until stuff is ready.
			R = fun(V) -> gen_server2:reply(From, V) end,
			{noreply, State#redis{reply_caller=R}}
	end;
handle_call({send, Cmd}, From, State1) ->
	% NOTE: redis ignores sent commands it doesn't understand, which means
	% we don't get a reply, which means callers will timeout
	State = ensure_started(State1),
	
	case gen_tcp:send(State#redis.socket, [Cmd | <<?EOL>>]) of
		ok ->
			Queue = queue:in(From, State#redis.calls),
			
			case Cmd of
				% TODO: is there a cleaner way of extracting select DB command
				% from multi-bulk commands?
				[_, [[<<"$">>, _, <<?EOL>>, <<"select">>, <<?EOL>>],
					 [<<"$">>, _, <<?EOL>>, DB, <<?EOL>>]]] ->
					{noreply, dont_reset_remaining(State, Queue, DB)};
				_ ->
					{noreply, dont_reset_remaining(State, Queue)}
			end;
		{error, Reason} ->
			%error_logger:error_report([{send, Cmd}, {error, Reason}]),
			{stop, timeout, {error, Reason}, State}
	end;
handle_call({subscribe, Cmd, Class, Pid}, From, State1)->
	State = ensure_started(State1),
	
	case gen_tcp:send(State#redis.socket, [Cmd | <<?EOL>>]) of
		ok ->
			Queue = queue:in(From, State#redis.calls),
			Subscribers = dict:store(Class, Pid, State#redis.subscribers),
			{noreply, State#redis{calls=Queue, remaining=1, subscribers=Subscribers}};
		{error, Reason} ->
			error_logger:error_report([{send, Cmd}, {error, Reason}]),
			{stop, timeout, {error, Reason}, State}
	end;
handle_call({unsubscribe, Cmd, Class}, From, State1)->
	State = ensure_started(State1),
	
	case gen_tcp:send(State#redis.socket, [Cmd | <<?EOL>>]) of
		ok ->
			Queue = queue:in(From, State#redis.calls),
			
			if
				Class == <<"">> ->
					Subscribers = dict:new();
				true ->
					Subscribers = dict:erase(Class, State#redis.subscribers)
			end,
			
			{noreply, State#redis{calls=Queue, remaining=1, subscribers=Subscribers}};
		{error, Reason} ->
			error_logger:error_report([{send, Cmd}, {error, Reason}]),
			{stop, timeout, {error, Reason}, State}
	end;
handle_call(disconnect, _, State) ->
	{stop, shutdown, shutdown, State};
handle_call(_, _, State) ->
	{reply, undefined, State}.

%%%%%%%%%%%%%%%%%
%% handle_cast %%
%%%%%%%%%%%%%%%%%

handle_cast({pipelining, Bool}, State) ->
	{noreply, State#redis{pipeline=Bool}};
handle_cast(disconnect, State) ->
	{stop, shutdown, State};
handle_cast({send, Cmd}, #redis{remaining=Remaining, calls=Calls} = State1) ->
	State = ensure_started(State1),
	Queue = queue:in(async, Calls),
	gen_tcp:send(State#redis.socket, [Cmd | <<?EOL>>]),
	
	case Remaining of
		0 -> {noreply, State#redis{remaining=1, calls=Queue}};
		_ -> {noreply, State#redis{calls=Queue}}
	end;
handle_cast(_, State) ->
	{noreply, State}.

%%%%%%%%%%%%%%%%%
%% handle_info %%
%%%%%%%%%%%%%%%%%

recv_value(Socket, NBytes) ->
	inet:setopts(Socket, [{packet, 0}]), % go into raw mode to read bytes
	
	case gen_tcp:recv(Socket, NBytes+2) of
		{ok, Packet} ->
%		  error_logger:error_report({line, Packet, NBytes}),
			inet:setopts(Socket, [{packet, line}]), % go back to line mode
			trim2(Packet);
		{error, Reason} ->
			error_logger:error_report([{recv, NBytes}, {error, Reason}]),
			throw({error, Reason})
	end.

send_reply(#redis{pipeline=true, calls=Calls, results=Results, reply_caller=ReplyCaller}=State)->
	case lists:reverse(State#redis.buffer) of
		[Result] when is_atom(Result) -> Result;
		Result -> Result
	end,
	
	{_, Queue} = queue:out(Calls),
	
	case queue:len(Queue) of
		0 ->
			FullResults = [Result|Results],
			
			NewState = case ReplyCaller of
				undefined ->
					State#redis{results=FullResults};
				_ ->
					ReplyCaller(lists:reverse(FullResults)),
					State#redis{results=[]}
			end,
			
			NewState#redis{
				remaining=0, pstate=empty, reply_caller=undefined,
				buffer=[], calls=Queue
			};
		_ ->
			State#redis{
				results=[Result|Results], remaining=1, pstate=empty,
				buffer=[], calls=Queue
			}
	end;

send_reply(State) ->
	case queue:out(State#redis.calls) of
		{{value, From}, Queue} ->
			Reply = lists:reverse(State#redis.buffer),
			gen_server2:reply(From, Reply);
		{empty, Queue} ->
			ok
	end,
	
	State#redis{calls=Queue, buffer=[], pstate=empty}.

parse_state(State, Socket, Data) ->
	Parse = erldis_proto:parse(State#redis.pstate, trim2(Data)),
%        error_logger:error_msg({trimmed, trim2(Data), parsed, Parse}),
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
		{N, {read, nil}} ->
			% reply with nil
			case State#redis.pstate of
				empty -> send_reply(State#redis{buffer=[nil]});
				read -> NewBuffer = [nil | State#redis.buffer],
					case N of
						0 -> send_reply(State#redis{buffer = NewBuffer});
						_ -> State#redis{remaining=N, buffer=NewBuffer, pstate=read}
					end
			end;
		{_, {read, 0}} when State#redis.pstate =:= empty ->
			% this is needed to handle single-line reply empty responses
			send_reply(State#redis{buffer=[]});
		{0, {read, NBytes}} ->
			% reply with Value added to buffer
			Value = recv_value(Socket, NBytes),
			
			case [Value | State#redis.buffer] of
				[PubSubValue, Class, <<"message">>] ->
					case dict:find(Class, State#redis.subscribers) of
						{ok, Pid} -> 
							Pid ! {message, Class, PubSubValue};
						_ ->
							error_logger:error_report([lost_message, {class, Class}])
					end,
					
					send_reply(State#redis{buffer=[]});
				Buffer -> 
					send_reply(State#redis{buffer=Buffer})
			end;
		{N, {read, NBytes}} ->
			% accumulate multi bulk reply
			Value = recv_value(Socket, NBytes),
			Buffer = [Value | State#redis.buffer],
			State#redis{remaining=N, buffer=Buffer, pstate=read};
		{0, Value} ->
			% reply with Value
			Buffer = [Value | State#redis.buffer],
			send_reply(State#redis{buffer=Buffer});
		{N, Value} ->
			Buffer = [Value | State#redis.buffer],
			State#redis{remaining=N, buffer=Buffer, pstate=read}
	end.

handle_info({tcp, Socket, Data}, State) ->
	%error_logger:error_report([{data, Data}, {state, State}]),
	case parse_state(State, Socket, Data) of
		{error, Reason} ->
			{stop, Reason, State};
		NewState ->
			inet:setopts(Socket, [{active, once}]),
			{noreply, NewState}
	end;
handle_info({tcp_closed, Socket}, State=#redis{socket=Socket}) ->
	{noreply, State#redis{socket=undefined}};
handle_info(_Info, State) ->
	{noreply, State}.

%%%%%%%%%%%%%%%
%% terminate %%
%%%%%%%%%%%%%%%

terminate(_Reason, State) ->
	% NOTE: if supervised with brutal_kill, may not be able to reply
	R = fun(From) -> gen_server2:reply(From, {error, closed}) end,
	lists:foreach(R, queue:to_list(State#redis.calls)),
	
	case State#redis.socket of
		undefined -> ok;
		Socket -> gen_tcp:close(Socket)
	end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
