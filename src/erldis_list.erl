-module(erldis_list).

% original queue
-export([is_queue/2, is_empty/2, len/2, in/3, in_r/3, out/2, out_r/2]).
% extra queue
-export([out_foreach/3]).
% extended queue
-export([get/2, get_r/2, drop/2, drop_r/2, peek/2, peek_r/2]).
% array
-export([get/3, is_array/2, set/4, size/2]).
% list
-export([delete/3, foreach/3, is_list/2, last/2, merge/4, nth/3,
		 sublist/3, sublist/4, umerge/4]).
% common
-export([foldl/4, foldr/4, from_list/3, to_list/2, reverse/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% original queue like api %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

is_queue(Key, Client) -> is_list(Key, Client).

is_empty(Key, Client) -> len(Key, Client) == 0.

len(Key, Client) -> erldis:llen(Client, Key).

in(Item, Key, Client) ->
	erldis:rpush(Client, Key, Item).

in_r(Item, Key, Client) ->
	erldis:lpush(Client, Key, Item).

out(Key, Client) ->
	case erldis:lpop(Client, Key) of
		nil -> empty;
		Item -> {value, Item}
	end.

out_r(Key, Client) ->
	case erldis:rpop(Client, Key) of
		nil -> empty;
		Item -> {value, Item}
	end.

%% @doc Call F on each element in queue until it's empty.
out_foreach(F, Key, Client) ->
	case out(Key, Client) of
		empty ->
			ok;
		{value, Item} ->
			F(Item),
			out_foreach(F, Key, Client)
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% extended queue like api %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get(Key, Client) ->
	case get(0, Key, Client) of
		nil -> empty;
		Item -> Item
	end.

get_r(Key, Client) ->
	Len = len(Key, Client),
	
	if
		Len < 1 ->
			empty;
		true ->
			case get(Len - 1, Key, Client) of
				nil -> empty;
				Item -> Item
			end
	end.

drop(Key, Client) -> out(Key, Client).

drop_r(Key, Client) -> out_r(Key, Client).

peek(Key, Client) ->
	case get(0, Key, Client) of
		nil -> empty;
		Item -> {value, Item}
	end.

peek_r(Key, Client) ->
	case get_r(Key, Client) of
		empty -> empty;
		Item -> {value, Item}
	end.

%%%%%%%%%%%%%%%%%%%%
%% array like api %%
%%%%%%%%%%%%%%%%%%%%

get(I, Key, Client) -> erldis:lindex(Client, Key, I).

is_array(Key, Client) -> is_list(Key, Client).

set(I, Value, Key, Client) -> erldis:lset(Client, Key, I, Value).

size(Key, Client) -> len(Key, Client).

%%%%%%%%%%%%%%%%%%%%
%% lists like api %%
%%%%%%%%%%%%%%%%%%%%

% all
% any
% append

delete(Elem, Key, Client) -> erldis:lrem(Client, Key, 1, Elem).

% dropwhile

foreach(F, Key, Client) -> foreach(0, F, Key, Client).

foreach(I, F, Key, Client) ->
	case get(I, Key, Client) of
		nil -> ok;
		Item -> F(Item), foreach(I+1, F, Key, Client)
	end.

is_list(Key, Client) -> <<"list">> == erldis:type(Client, Key).

% keysort

last(Key, Client) -> get_r(Key, Client).

merge(F, L, Key, Client) -> merge(0, F, L, Key, Client).

merge(_, _, [], _, _) ->
	ok;
merge(I, F, L, Key, Client) ->
	case get(I, Key, Client) of
		% append the rest of the list
		nil -> lists:foreach(fun(Item) -> in(Item, Key, Client) end, L);
		% compare A to head of L
		A -> merge(I, F, A, L, Key, Client)
	end.

merge(I, F, A, [B | L], Key, Client) ->
	case F(A, B) of
		true ->
			% B comes after A, so continue iterating
			merge(I+1, F, [B | L], Key, Client);
		false ->
			% B comes before A, so replace A with B and continue merging with
			% A merged into L
			set(I, B, Key, Client),
			merge(I+1, F, lists:merge(F, [A], L), Key, Client)
	end.

nth(N, Key, Client) -> get(N, Key, Client).

% nthtail

sublist(Key, Client, Len) -> sublist(Key, Client, 1, Len).

sublist(Key, Client, Start, 1) ->
	case get(Start, Key, Client) of
		nil -> [];
		Elem -> [Elem]
	end;
sublist(Key, Client, Start, Len) when Start > 0, Len > 1 ->
	% erlang lists are 1-indexed
	erldis:lrange(Client, Key, Start - 1, Start + Len - 2);
sublist(Key, Client, Start, Len) when Start < 0, Len > 1 ->
	% can give a negative start where -1 is the last element
	erldis:lrange(Client, Key, Start, Start - Len + 1).

% sort
% takewhile

umerge(F, L, Key, Client) -> umerge(0, F, L, Key, Client).

umerge(_, _, [], _, _) ->
	ok;
umerge(I, F, L, Key, Client) ->
	case get(I, Key, Client) of
		nil -> lists:foreach(fun(Item) -> in(Item, Key, Client) end, L);
		A -> umerge(I, F, A, L, Key, Client)
	end.

umerge(I, F, A, [B | L], Key, Client) when A == B ->
	umerge(I+1, F, L, Key, Client);
umerge(I, F, A, [B | L], Key, Client) ->
	case F(A, B) of
		true ->
			umerge(I+1, F, [B | L], Key, Client);
		false ->
			set(I, B, Key, Client),
			umerge(I+1, F, lists:umerge(F, [A], L), Key, Client)
	end.

%%%%%%%%%%%%
%% common %%
%%%%%%%%%%%%

% TODO: foldl, foldr, to_list, foreach, and any other iterative functions
% would probably be much faster and more efficient if they iterated on
% fixed sized chunks using sublist, since each call to get/3 is avg O(n)
% except when I is the first or last element.

foldl(F, Acc0, Key, Client) -> foldl(0, F, Acc0, Key, Client).

foldl(I, F, Acc0, Key, Client) ->
	case get(I, Key, Client) of
		nil -> Acc0;
		Item -> foldl(I+1, F, F(Item, Acc0), Key, Client)
	end.

foldr(F, Acc0, Key, Client) -> foldr(len(Key, Client) - 1, F, Acc0, Key, Client).

foldr(I, _, Acc0, _, _) when I < 0 ->
	Acc0;
foldr(I, F, Acc0, Key, Client) ->
	case get(I, Key, Client) of
		nil -> Acc0;
		Item -> foldr(I-1, F, F(Item, Acc0), Key, Client)
	end.

from_list(L, Key, Client) ->
	erldis:del(Client, Key),
	lists:foreach(fun(Item) -> in(Item, Key, Client) end, L).

to_list(Key, Client) -> foldr(fun(Item, L) -> [Item | L] end, [], Key, Client).

reverse(Key, Client) -> foldl(fun(Item, L) -> [Item | L] end, [], Key, Client).

% filter
% map
% member
