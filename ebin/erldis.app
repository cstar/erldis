{application, erldis, [
	{description, "Erlang Redis application"},
	{vsn, "0.0.1"},
	{registered, [erldis_sup]},
	{mod, {erldis_app, []}},
	% TODO: include eunit?
	{applications, [kernel, stdlib]},
	{modules, [erldis_client, erldis, erldis_proto, erldis_app, erldis_sup]},
	{env, [{host, "localhost"}, {port, 6379}, {timeout, 500}]}
]}.