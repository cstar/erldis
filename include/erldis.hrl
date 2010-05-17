-record(redis, {
	socket, buffer=[], reply_caller, pipeline=false, calls=0, remaining=0,
	pstate=empty, results=[], host, port, timeout, db = <<"0">>, subscribers
}).

-define(EOL, "\r\n").
