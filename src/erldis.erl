-module(erldis).

-compile(export_all).
-define(EOL, "\r\n").

%% helpers
flatten({error, Message}) ->
	{error, Message};
flatten(List) when is_list(List)->	 
	lists:flatten(List).

%% exposed API
connect() ->
	erldis_client:connect().
connect(Host) ->
	erldis_client:connect(Host).
connect(Host, Port) ->
	erldis_client:connect(Host, Port).
connect(Host, Port, Options) ->
	erldis_client:connect(Host, Port, Options).

get_all_results(Client) -> gen_server2:call(Client, get_all_results).

set_pipelining(Client, Bool) -> gen_server2:cast(Client, {pipelining, Bool}).

quit(Client) ->
	erldis_client:scall(Client, <<"QUIT">>),
	erldis_client:disconnect(Client).

%% Commands operating on string values
internal_set_like(Client, Command, Key, Value) when is_binary(Key), is_binary(Value) ->
	Size = list_to_binary(integer_to_list(size(Value))),
	Cmd = <<Command/binary, Key/binary, " ", Size/binary, "\r\n", Value/binary>>,
	
	case erldis_client:call(Client, Cmd) of
		[{error, _}=Error] -> Error;
		[R] when R == ok; R == nil; R == true; R == false -> R;
		R -> R
	end;
internal_set_like(Client, Command, Key, Value) when is_binary(Value) ->
	case erldis_client:call(Client, Command, [[Key, size(Value)], [Value]]) of
		[{error, _}=Error] -> Error;
		[R] when R == ok; R == nil; R == true; R == false -> R;
		R -> R
	end;
internal_set_like(Client, Command, Key, Value) when not is_binary(Key) ->
	internal_set_like(Client, Command, erldis_client:bin(Key), Value);
internal_set_like(Client, Command, Key, Value) when not is_binary(Value) ->
	internal_set_like(Client, Command, Key, erldis_client:bin(Value));
internal_set_like(_, _, _, _) ->
	{error, badarg}.

auth(Client, Password) ->
	erldis_client:scall(Client, <<"auth ", Password/binary>>).

exec(Client, Fun) ->
	case erldis_client:sr_scall(Client, <<"multi ">>) of
		ok ->
			set_pipelining(Client, true),
			Fun(Client),
			get_all_results(Client),
			set_pipelining(Client, false),
			erldis_client:scall(Client, <<"exec ">>);
		_ ->
			{error, unsupported}
	end.

numeric(false) -> 0;
numeric(true) -> 1;
numeric(I) -> I.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on every value %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

exists(Client, Key) when not is_binary(Key) ->
	exists(Client, erldis_client:bin(Key));
exists(Client, Key) ->
	erldis_client:sr_scall(Client, <<"exists ", Key/binary>>).

del(Client, Key) when not is_binary(Key) ->
	del(Client, erldis_client:bin(Key));
del(Client, Key) ->
	erldis_client:sr_scall(Client, <<"del ", Key/binary>>).

type(Client, Key) when not is_binary(Key) ->
	type(Client, erldis_client:bin(Key));
type(Client, Key) ->
	erldis_client:sr_scall(Client, <<"type ", Key/binary>>).

keys(Client, Pattern) when not is_binary(Pattern) ->
	keys(Client, erldis_client:bin(Pattern));
keys(Client, Pattern) ->
	% TODO: tokenize the binary directly (if is faster)
	% NOTE: with binary-list conversion, timer:tc says 26000-30000 microseconds
	case erldis_client:scall(Client, <<"keys ">>, [Pattern]) of
		[] -> [];
		[B] -> [list_to_binary(S) || S <- string:tokens(binary_to_list(B), " ")]
	end.

randomkey(Client, Key) when not is_binary(Key) ->
	randomkey(Client, erldis_client:bin(Key));
randomkey(Client, Key) ->
	erldis_client:sr_scall(Client, <<"randomkey ", Key/binary>>).

rename(Client, OldKey, NewKey) when not is_binary(OldKey) ->
	rename(Client, erldis_client:bin(OldKey), NewKey);
rename(Client, OldKey, NewKey) when not is_binary(NewKey) ->
	rename(Client, OldKey, erldis_client:bin(NewKey));
rename(Client, OldKey, NewKey) ->
	erldis_client:sr_scall(Client, <<"rename ", OldKey/binary, " ", NewKey/binary>>).

renamenx(Client, OldKey, NewKey) when not is_binary(OldKey) ->
	renamenx(Client, erldis_client:bin(OldKey), NewKey);
renamenx(Client, OldKey, NewKey) when not is_binary(NewKey) ->
	renamenx(Client, OldKey, erldis_client:bin(NewKey));
renamenx(Client, OldKey, NewKey) ->
	erldis_client:sr_scall(Client, <<"renamenx ", OldKey/binary, " ", NewKey/binary>>).

dbsize(Client) -> numeric(erldis_client:sr_scall(Client, <<"dbsize ">>)).

expire(Client, Key, Seconds) when not is_binary(Key) ->
	expire(Client, erldis_client:bin(Key), Seconds);
expire(Client, Key, Seconds) ->
	erldis_client:sr_scall(Client, <<"expire ", Key/binary, " ", Seconds/binary>>).

ttl(Client, Key) -> erldis_client:sr_scall(Client, <<"ttl ", Key/binary>>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on string values %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

set(Client, Key, Value) -> internal_set_like(Client, <<"set ">>, Key, Value).

get(Client, Key) when not is_binary(Key) ->
	get(Client, erldis_client:bin(Key));
get(Client, Key) ->
	erldis_client:sr_scall(Client, <<"get ", Key/binary>>).

getset(Client, Key, Value) -> internal_set_like(Client, <<"getset ">>, Key, Value).

mget(Client, Keys) -> erldis_client:scall(Client, <<"mget ">>, Keys).

setnx(Client, Key, Value) -> internal_set_like(Client, <<"setnx ">>, Key, Value).

incr(Client, Key) when not is_binary(Key) ->
	incr(Client, erldis_client:bin(Key));
incr(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, <<"incr ", Key/binary>>)).

incrby(Client, Key, By) ->
	numeric(erldis_client:sr_scall(Client, <<"incrby ">>, [Key, By])).

decr(Client, Key) when not is_binary(Key) ->
	decr(Client, erldis_client:bin(Key));
decr(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, <<"decr ", Key/binary>>)).

decrby(Client, Key, By) ->
	numeric(erldis_client:sr_scall(Client, <<"decrby ">>, [Key, By])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on lists %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

rpush(Client, Key, Value) -> internal_set_like(Client, <<"rpush ">>, Key, Value).

lpush(Client, Key, Value) -> internal_set_like(Client, <<"lpush ">>, Key, Value).

llen(Client, Key) -> numeric(erldis_client:sr_scall(Client, <<"llen ">>, [Key])).

lrange(Client, Key, Start, End) ->
	erldis_client:scall(Client, <<"lrange ">>, [Key, Start, End]).

ltrim(Client, Key, Start, End) ->
	erldis_client:scall(Client, <<"ltrim ">>, [Key, Start, End]).
	
lindex(Client, Key, Index) ->
	erldis_client:sr_scall(Client, <<"lindex ">>, [Key, Index]).

lset(Client, Key, Index, Value) when not is_binary(Value) ->
	lset(Client, Key, Index, erldis_client:bin(Value));
lset(Client, Key, Index, Value) ->
	erldis_client:call(Client, <<"lset ">>, [[Key, Index, size(Value)], [Value]]).

lrem(Client, Key, Number, Value) when not is_binary(Value) ->
	lrem(Client, Key, Number, erldis_client:bin(Value));
lrem(Client, Key, Number, Value) ->
	erldis_client:call(Client, <<"lrem ">>, [[Key, Number, size(Value)], [Value]]).
	
lpop(Client, Key) -> erldis_client:sr_scall(Client, <<"lpop ">>, [Key]).

rpop(Client, Key) -> erldis_client:sr_scall(Client, <<"rpop ">>, [Key]).

blpop(Client, Keys) -> erldis_client:bcall(Client, <<"blpop ">>, Keys, infinity).
blpop(Client, Keys, Timeout) -> erldis_client:bcall(Client, <<"blpop ">>, Keys, Timeout).

brpop(Client, Keys) -> erldis_client:bcall(Client, <<"brpop ">>, Keys, infinity).
brpop(Client, Keys, Timeout) -> erldis_client:bcall(Client, <<"brpop ">>, Keys, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on sets %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sadd(Client, Key, Value) -> internal_set_like(Client, <<"sadd ">>, Key, Value).

srem(Client, Key, Value) -> internal_set_like(Client, <<"srem ">>, Key, Value).

smove(Client, SrcKey, DstKey, Member) when not is_binary(Member) ->
	smove(Client, SrcKey, DstKey, erldis_client:bin(Member));
smove(Client, SrcKey, DstKey, Member) ->
	erldis_client:call(Client, <<"smove ">>, [[SrcKey, DstKey, size(Member)], [Member]]).

scard(Client, Key) -> numeric(erldis_client:sr_scall(Client, <<"scard ">>, [Key])).

sismember(Client, Key, Value) -> internal_set_like(Client, <<"sismember ">>, Key, Value).

sintersect(Client, Keys) -> erldis_client:scall(Client, <<"sinter ">>, Keys).

sinter(Client, Keys) -> sintersect(Client, Keys).

sinterstore(Client, DstKey, Keys) ->
	erldis_client:scall(Client, <<"sinterstore ">>, [DstKey|Keys]).

sunion(Client, Keys) -> erldis_client:scall(Client, <<"sunion ">>, Keys).

sunionstore(Client, DstKey, Keys) ->
	erldis_client:scall(Client, <<"sunionstore ">>, [DstKey|Keys]).

sdiff(Client, Keys) -> erldis_client:scall(Client, <<"sdiff ">>, Keys).

sdiffstore(Client, DstKey, Keys) ->
	erldis_client:scall(Client, <<"sdiffstore ">>, [DstKey|Keys]).

smembers(Client, Key) -> erldis_client:scall(Client, <<"smembers ">>, [Key]).

% TODO: ordered sets

%%%%%%%%%%%%%
%% Sorting %%
%%%%%%%%%%%%%

sort(Client, Key) -> erldis_client:scall(Client, <<"sort ">>, [Key]).

sort(Client, Key, Extra) -> erldis_client:scall(Client, <<"sort ">>, [Key, Extra]).	

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Multiple DB commands %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

select(Client, Index) -> erldis_client:sr_scall(Client, <<"select ">>, [Index]).

move(Client, Key, DBIndex) ->
	erldis_client:scall(Client, <<"move ">>, [Key, DBIndex]).

flushdb(Client) -> erldis_client:sr_scall(Client, <<"flushdb ">>).

flushall(Client) -> erldis_client:sr_scall(Client, <<"flushall ">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Persistence control commands %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

save(Client) -> erldis_client:scall(Client, <<"save ">>).

bgsave(Client) -> erldis_client:scall(Client, <<"bgsave ">>).

lastsave(Client) -> erldis_client:scall(Client, <<"lastsave ">>).

shutdown(Client) -> erldis_client:scall(Client, <<"shutdown ">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Remote server control commands %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

info(Client) -> erldis_client:scall(Client, <<"info ">>).

slaveof(Client, Host, Port) ->
	erldis_client:scall(Client, <<"slaveof ">>, [Host, Port]).

slaveof(Client) -> erldis_client:scall(Client, <<"slaveof ">>, ["no one"]).
