-module(erldis_proto).

-include("erldis.hrl").

-export([multibulk_cmd/1, parse/2, parse_stat/1]).

%%%%%%%%%%%%%%%%%%
%% send command %%
%%%%%%%%%%%%%%%%%%

-define(i2l(X), integer_to_list(X)).

multibulk_cmd(Args) when is_binary(Args) ->
	multibulk_cmd([Args]);
multibulk_cmd(Args) when is_list(Args) ->
	TotalLength = length(Args),
	ArgCount = [<<"*">>, ?i2l(TotalLength), <<?EOL>>],
	Bins = [erldis_binaries:to_binary(B) || B <- Args],
	ArgBin = [[<<"$">>, ?i2l(iolist_size(A)), <<?EOL>>, A, <<?EOL>>] || A <- Bins],
	[ArgCount, ArgBin].

%%%%%%%%%%%%%%%%%%%%
%% parse response %%
%%%%%%%%%%%%%%%%%%%%

parse(_, <<"+OK">>) ->
	ok;
parse(_, <<":0">>) ->
	false;
parse(_, <<":1">>) ->
	true;
parse(empty, <<"+QUEUED">>) ->
	queued;
parse(empty, <<"+PONG">>) ->
	pong;
parse(empty, <<"-", Message/binary>>) ->
    {error, Message};
parse(_, <<"$-1">>) ->
    {read, nil};
parse(empty, <<"*-1">>) ->
	{hold, nil};
parse(empty, <<"*0">>) ->
	{read, 0};
parse(_, <<"$", BulkSize/binary>>) ->
	{read, list_to_integer(binary_to_list(BulkSize))};
parse(empty, <<"*", MultiBulkSize/binary>>) ->
	{hold, list_to_integer(binary_to_list(MultiBulkSize))};
parse(read, Message)->
	convert(Message);
parse(empty, Message) ->
	convert(Message).

convert(<<":", Message/binary>>) ->
	list_to_integer(binary_to_list(Message));
% in case the message is not OK or PONG it's a
% real value that we don't know how to convert
% so just pass it as is and remove the +
convert(<<"+", Message/binary>>) ->
	Message;
convert(Message) ->
	Message.

%%%%%%%%%%%%%%%%%%%%%%%%%
%% parse info response %%
%%%%%%%%%%%%%%%%%%%%%%%%%

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
