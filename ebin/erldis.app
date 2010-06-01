{application, erldis, [
	{description, "Erlang Redis application"},
	{vsn, "0.3.1"},
	{registered, [erldis_sup]},
	{mod, {erldis_app, []}},
	{applications, [kernel, stdlib]},
	{modules, [
		erldis_client, erldis, erldis_proto, erldis_app, erldis_sup,
		erldis_sets, erldis_dict, erldis_list, gen_server2, erldis_sync_client,
		erldis_binaries
	]},
	{env, [{host, "localhost"}, {port, 6379}, {timeout, 500}]}
]}.
