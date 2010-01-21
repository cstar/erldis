-module(erldis_dict).

-export([append/3, append_list/3, erase/2, fetch/2, fetch_keys/2, find/2,
		 is_key/2, size/1, store/3, update/3, update/4,
		 update_counter/2, update_counter/3]).

% NOTE: use erldis_lists instead, fetch & find won't work for lists
append(Key, Value, Client) -> erldis:rpush(Client, Key, Value).

append_list(Key, Values, Client) ->
	lists:foreach(fun(Value) -> append(Key, Value, Client) end, Values).

erase(Key, Client) -> erldis:del(Client, Key).

fetch(Key, Client) ->
	case erldis:get(Client, Key) of
		nil -> undefined;
		Value -> Value
	end.

% NOTE: this is only useful if keys have a known prefix
fetch_keys(Pattern, Client) -> erldis:keys(Client, Pattern).

%filter(Pred, Client) -> ok.

find(Key, Client) ->
	case fetch(Key, Client) of
		undefined -> error;
		Value -> {ok, Value}
	end.

%fold(Fun, Acc0, Client) -> ok.

%from_list(List, Client) -> ok.

is_key(Key, Client) -> erldis:exists(Client, Key).

size(Client) -> erldis:dbsize(Client).

store(Key, [], Client) -> erase(Key, Client);
store(Key, Value, Client) -> erldis:set(Client, Key, Value).

%to_list(Client) -> ok.

% NOTE: update/3 & update/4 are not atomic

update(Key, Fun, Client) -> store(Key, Fun(fetch(Key, Client)), Client).

update(Key, Fun, Initial, Client) ->
	case find(Key, Client) of
		{ok, Value} -> store(Key, Fun(Value), Client);
		error -> store(Key, Initial, Client)
	end.

update_counter(Key, Client) -> update_counter(Key, 1, Client).

% NOTE: this returns new count value, not a modified dict
update_counter(Key, 1, Client) -> erldis:incr(Client, Key);
update_counter(Key, Incr, Client) -> erldis:incrby(Client, Key, Incr).