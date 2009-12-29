%% @doc sets like interface to redis. Uses erldis_client to ensure
%% synchronous results.
%%
%% @author Jacob Perkins <japerk@gmail.com>
-module(erldis_sets).

-export([delete/1, is_set/2, size/2, to_list/2, from_list/3, is_element/3,
		 add_element/3, del_element/3, union/2, intersection/3, intersection/2,
		 is_disjoint/3, subtract/3, subtract/2, is_subset/3, fold/4, filter/3]).

%%%%%%%%%%%%%%%%%%%
%% sets-like api %%
%%%%%%%%%%%%%%%%%%%

delete(Client) -> erldis_client:stop(Client).

is_set(Client, Key) -> [<<"set">>] == scall(Client, <<"type ">>, [Key]).

size(Client, Key) ->
	case scall(Client, <<"scard ">>, [Key]) of
		% redis actually returns 0 & 1, but those are interpreted as false & true
		[false] -> 0;
		[true] -> 1;
		[Size] -> Size
	end.

to_list(Client, Key) -> scall(Client, <<"smembers ">>, [Key]).

from_list(Client, Key, List) ->
	% delete existing set
	scall(Client, <<"del ">>, [Key]),
	lists:foreach(fun(Elem) -> add_element(Elem, Client, Key) end, List),
	Client.

is_element(Elem, Client, Key) ->
	case set_call(Client, <<"sismember ">>, Key, Elem) of
		[false] -> false;
		[true] -> true
	end.

add_element(Elem, Client, Key) -> set_call(Client, <<"sadd ">>, Key, Elem).

del_element(Elem, Client, Key) -> set_call(Client, <<"srem ">>, Key, Elem).

union(Client, Keys) -> scall(Client, <<"sunion ">>, Keys).

intersection(Client, Key1, Key2) -> intersection(Client, [Key1, Key2]).

intersection(Client, Keys) -> scall(Client, <<"sinter ">>, Keys).

is_disjoint(Client, Key1, Key2) -> [] == intersection(Client, [Key1, Key2]).

subtract(Client, Key1, Key2) -> subtract(Client, [Key1, Key2]).

subtract(Client, Keys) -> scall(Client, <<"sdiff ">>, Keys).

is_subset(Client, Key1, Key2) -> [] == subtract(Client, [Key2, Key1]).

fold(F, Acc0, Client, Key) -> lists:foldl(F, Acc0, to_list(Client, Key)).

filter(Pred, Client, Key) -> lists:filter(Pred, to_list(Client, Key)).

%%%%%%%%%%%%%
%% helpers %%
%%%%%%%%%%%%%

% TODO: handle {error, Reason}. throw exception?

scall(Client, Cmd, Args) -> erldis_client:scall(Client, Cmd, Args).

set_call(Client, Cmd, Key, Val) ->
	erldis_client:call(Client, Cmd, [[Key, erlang:size(Val)], [Val]]).