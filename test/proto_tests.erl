-module(proto_tests).

-include_lib("eunit/include/eunit.hrl").

parse_test() ->
    ok = erldis_proto:parse(empty, <<"+OK">>),
    pong = erldis_proto:parse(empty, <<"+PONG">>),
    false = erldis_proto:parse(empty, <<":0">>),
    true = erldis_proto:parse(empty, <<":1">>),
    {error, <<"1">>} = erldis_proto:parse(empty, <<"-1">>).
