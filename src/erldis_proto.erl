-module(erldis_proto).

-export([parse/2]).

parse(read, <<"+OK">>) ->
    ok;
parse(empty, <<"+OK">>) ->
    ok;
parse(empty, <<"+QUEUED">>) ->
    ok;
parse(empty, <<"+PONG">>) ->
    pong;
parse(empty, <<":0">>) ->
    false;
parse(empty, <<":1">>) ->
    true;
parse(empty, <<"-",Message/binary>>) ->
    {error, Message};
parse(empty, <<"$-1">>) ->
    {read, nil};
parse(empty, <<"*-1">>) ->
    {hold, nil};
parse(empty, <<"*0">>) ->
    {read, 0};
parse(empty, <<"$",BulkSize/binary>>) ->
    {read, list_to_integer(binary_to_list(BulkSize))};
parse(read, <<"$",BulkSize/binary>>) ->
    {read, list_to_integer(binary_to_list(BulkSize))};
parse(empty, <<"*",MultiBulkSize/binary>> )->
    {hold, list_to_integer(binary_to_list(MultiBulkSize))};
parse(empty, Message) ->
    convert(Message).

convert(<<":",Message/binary>>) ->
    list_to_integer(binary_to_list(Message));
% in case the message is not OK or PONG it's a
% real value that we don't know how to convert
% to an atom, so just pass it as is and remove
% the +
convert(<<"+",Message/binary>>) -> 
    Message;
convert(Message) ->
    Message.

