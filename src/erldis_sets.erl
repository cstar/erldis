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

is_set(Client, Key) -> <<"set">> == erldis:type(Client, Key).

size(Client, Key) -> erldis:scard(Client, Key).

to_list(Client, Key) -> erldis:smembers(Client, Key).

from_list(Client, Key, List) ->
	% delete existing set
	erldis:del(Client, Key),
	lists:foreach(fun(Elem) -> add_element(Elem, Client, Key) end, List),
	Client.

is_element(Elem, Client, Key) -> erldis:sismember(Client, Key, Elem).

add_element(Elem, Client, Key) -> erldis:sadd(Client, Key, Elem).

del_element(Elem, Client, Key) -> erldis:srem(Client, Key, Elem).

union(Client, Keys) -> erldis:sunion(Client, Keys).

intersection(Client, Key1, Key2) -> intersection(Client, [Key1, Key2]).

intersection(Client, Keys) -> erldis:sintersect(Client, Keys).

is_disjoint(Client, Key1, Key2) -> [] == intersection(Client, [Key1, Key2]).

subtract(Client, Key1, Key2) -> subtract(Client, [Key1, Key2]).

subtract(Client, Keys) -> erldis:sdiff(Client, Keys).

is_subset(Client, Key1, Key2) -> [] == subtract(Client, [Key2, Key1]).

fold(F, Acc0, Client, Key) -> lists:foldl(F, Acc0, to_list(Client, Key)).

filter(Pred, Client, Key) -> lists:filter(Pred, to_list(Client, Key)).
