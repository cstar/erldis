-module(erldis_dict_tests).

-include_lib("eunit/include/eunit.hrl").

dict_test() ->
	% setup
	application:load(erldis),
	{ok, Client} = erldis_client:connect(),
	?assertEqual(erldis:flushdb(Client), ok),
	% empty dict
	?assertEqual(0, erldis_dict:size(Client)),
	?assertEqual(undefined, erldis_dict:fetch(<<"foo">>, Client)),
	?assertEqual(error, erldis_dict:find(<<"foo">>, Client)),
	?assertEqual(false, erldis_dict:is_key(<<"foo">>, Client)),
	% add element
	erldis_dict:store(<<"foo">>, <<"bar">>, Client),
	?assertEqual(<<"bar">>, erldis_dict:fetch(<<"foo">>, Client)),
	?assertEqual({ok, <<"bar">>}, erldis_dict:find(<<"foo">>, Client)),
	?assertEqual(1, erldis_dict:size(Client)),
	?assertEqual(true, erldis_dict:is_key(<<"foo">>, Client)),
	% del element
	erldis_dict:erase(<<"foo">>, Client),
	?assertEqual(error, erldis_dict:find(<<"foo">>, Client)),
	?assertEqual(0, erldis_dict:size(Client)),
	?assertEqual(false, erldis_dict:is_key(<<"foo">>, Client)),
	% update counter
	?assertEqual(1, erldis_dict:update_counter(<<"count">>, Client)),
	?assertEqual(3, erldis_dict:update_counter(<<"count">>, 2, Client)),
	erldis_dict:erase(<<"count">>, Client),
	erldis_client:stop(Client).