-module(erldis_multiexec_tests).

-include_lib("eunit/include/eunit.hrl").

me_test()->	
  application:load(erldis),
	{ok, Client} = erldis_sync_client:connect(),
	Fun = fun(C)->
	  erldis:set(C, <<"toto">>, <<"tata">>),
	  erldis:set(C, <<"toto2">>, <<"tata2">>),
	  erldis:get(C, <<"toto">>)
	end,
	?assertEqual([ok, ok, <<"tata">>],erldis:exec(Client, Fun)),
	Fun2 = fun(C)->
	  erldis:set(C, <<"foo">>, <<"bar">>),
	  erldis:set(C, <<"foo2">>, <<"bar2">>),
	  erldis:set(C, <<"foo3">>, <<"bar3">>)
	end,
	?assertEqual([ok, ok, ok],erldis:exec(Client, Fun2)).