-module(erldis).

-compile(export_all).

%%%%%%%%%%%%%%%%%%%%%%%
%% Client Connection %%
%%%%%%%%%%%%%%%%%%%%%%%

connect() -> erldis_client:connect().

connect(Host) -> erldis_client:connect(Host).

connect(Host, Port) -> erldis_client:connect(Host, Port).

connect(Host, Port, Options) -> erldis_client:connect(Host, Port, Options).

quit(Client) -> erldis_client:stop(Client).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on every value %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

exists(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"exists">>, Key)).

del(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"del">>, Key)).

type(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"type">>, Key)).

keys(Client, Pattern) ->
	% TODO: tokenize the binary directly (if is faster)
	% NOTE: with binary-list conversion, timer:tc says 26000-30000 microseconds
	case erldis_client:scall(Client, inline_cmd(<<"keys">>, Pattern)) of
		[] -> [];
		[B] -> [list_to_binary(S) || S <- string:tokens(binary_to_list(B), " ")]
	end.

% TODO: test randomkey, rename, renamenx, dbsize, expire, ttl

randomkey(Client, Key) ->
	erldis_client:sr_scall(Client, inline_cmd(<<"randomkey">>, Key)).

rename(Client, OldKey, NewKey) ->
	erldis_client:sr_scall(Client, inline_cmd([<<"rename">>, OldKey, NewKey])).

renamenx(Client, OldKey, NewKey) ->
	erldis_client:sr_scall(Client, inline_cmd([<<"renamenx">>, OldKey, NewKey])).

dbsize(Client) -> numeric(erldis_client:sr_scall(Client, <<"dbsize">>)).

expire(Client, Key, Seconds) ->
	erldis_client:sr_scall(Client, inline_cmd([<<"expire">>, Key, Seconds])).

ttl(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"ttl">>, Key)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on string values %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

set(Client, Key, Value) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"set">>, Key], Value)).

get(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"get">>, Key)).

getset(Client, Key, Value) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"getset">>, Key], Value)).

mget(Client, Keys) -> erldis_client:scall(Client, inline_cmd([<<"mget">> | Keys])).

setnx(Client, Key, Value) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"setnx">>, Key], Value)).

incr(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd(<<"incr">>, Key))).

incrby(Client, Key, By) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd([<<"incrby">>, Key, By]))).

decr(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd(<<"decr">>, Key))).

decrby(Client, Key, By) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd([<<"decrby">>, Key, By]))).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on lists %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

rpush(Client, Key, Value) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"rpush">>, Key], Value)).

lpush(Client, Key, Value) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"lpush">>, Key], Value)).

llen(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd(<<"llen">>, Key))).

lrange(Client, Key, Start, End) ->
	erldis_client:scall(Client, inline_cmd([<<"lrange">>, Key, Start, End])).

ltrim(Client, Key, Start, End) ->
	erldis_client:sr_scall(Client, inline_cmd([<<"ltrim">>, Key, Start, End])).
	
lindex(Client, Key, Index) ->
	erldis_client:sr_scall(Client, inline_cmd([<<"lindex">>, Key, Index])).

lset(Client, Key, Index, Value) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"lset">>, Key, Index], Value)).

lrem(Client, Key, Number, Value) ->
	numeric(erldis_client:sr_scall(Client, bulk_cmd([<<"lrem">>, Key, Number], Value))).

lpop(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"lpop">>, Key)).

rpop(Client, Key) -> erldis_client:sr_scall(Client, inline_cmd(<<"rpop">>, Key)).

% TODO: inline_cmd
blpop(Client, Keys) -> erldis_client:bcall(Client, <<"blpop ">>, Keys, infinity).
blpop(Client, Keys, Timeout) -> erldis_client:bcall(Client, <<"blpop ">>, Keys, Timeout).

brpop(Client, Keys) -> erldis_client:bcall(Client, <<"brpop ">>, Keys, infinity).
brpop(Client, Keys, Timeout) -> erldis_client:bcall(Client, <<"brpop ">>, Keys, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on sets %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

sadd(Client, Key, Member) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"sadd">>, Key], Member)).

srem(Client, Key, Member) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"srem">>, Key], Member)).

% TODO: test
smove(Client, SrcKey, DstKey, Member) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"smove">>, SrcKey, DstKey], Member)).

scard(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd(<<"scard ">>, Key))).

sismember(Client, Key, Member) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"sismember">>, Key], Member)).

sintersect(Client, Keys) -> sinter(Client, Keys).

sinter(Client, Keys) -> erldis_client:scall(Client, inline_cmd([<<"sinter">> | Keys])).

sinterstore(Client, DstKey, Keys) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd([<<"sinterstore">>, DstKey | Keys]))).

sunion(Client, Keys) ->
	erldis_client:scall(Client, inline_cmd([<<"sunion">> | Keys])).

sunionstore(Client, DstKey, Keys) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd([<<"sunionstore">>, DstKey | Keys]))).

sdiff(Client, Keys) -> erldis_client:scall(Client, inline_cmd([<<"sdiff">> | Keys])).

sdiffstore(Client, DstKey, Keys) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd([<<"sdiffstore">>, DstKey | Keys]))).

smembers(Client, Key) ->
	erldis_client:scall(Client, inline_cmd(<<"smembers">>, Key)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Commands operating on ordered sets %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

zadd(Client, Key, Score, Member) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"zadd">>, Key, Score], Member)).

zrem(Client, Key, Member) ->
	erldis_client:sr_scall(Client, bulk_cmd([<<"zrem">>, Key], Member)).

zincrby(Client, Key, By, Member) ->
	numeric(erldis_client:sr_scall(Client, bulk_cmd([<<"zincrby">>, Key, By], Member))).

zrange(Client, Key, Start, End) ->
	erldis_client:scall(Client, inline_cmd([<<"zrange">>, Key, Start, End])).

% TODO: return [{member, score}] for withscores functions
%zrange_withscores(Client, Key, Start, End) ->
%	erldis_client:scall(Client, <<"zrange ">>, [Key, Start, End, <<"withscores">>]).

zrevrange(Client, Key, Start, End) ->
	erldis_client:scall(Client, inline_cmd([<<"zrevrange">>, Key, Start, End])).

%zrevrange_withscores(Client, Key, Start, End) ->
%	erldis_client:scall(Client, <<"zrevrange ">>, [Key, Start, End, <<"withscores">>]).

zrangebyscore(Client, Key, Min, Max) ->
	erldis_client:scall(Client, inline_cmd([<<"zrangebyscore ">>, Key, Min, Max])).

zrangebyscore(Client, Key, Min, Max, Offset, Count) ->
	Cmd = inline_cmd([<<"zrangebyscore ">>, Key, Min, Max, <<"limit">>, Offset, Count]),
	erldis_client:scall(Client, Cmd).

zcard(Client, Key) ->
	numeric(erldis_client:sr_scall(Client, inline_cmd(<<"zcard">>, Key))).

zscore(Client, Key, Member) ->
	numeric(erldis_client:sr_scall(Client, bulk_cmd([<<"zscore">>, Key], Member))).

zremrangebyscore(Client, Key, Min, Max) ->
	Cmd = inline_cmd([<<"zremrangebyscore ">>, Key, Min, Max]),
	numeric(erldis_client:sr_scall(Client, Cmd)).

%%%%%%%%%%%%%
%% Sorting %%
%%%%%%%%%%%%%

sort(Client, Key) -> erldis_client:scall(Client, inline_cmd([<<"sort">>, Key])).

% TODO: better support for Extra options (LIMIT, ASC|DESC, BY, GET, STORE)
sort(Client, Key, Extra) ->
	erldis_client:scall(Client, inline_cmd([<<"sort">>, Key, Extra])).	

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Multiple DB commands %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

select(Client, Index) ->
	erldis_client:sr_scall(Client, inline_cmd(<<"select">>, Index)).

move(Client, Key, DBIndex) ->
	erldis_client:sr_scall(Client, inline_cmd([<<"move">>, Key, DBIndex])).

flushdb(Client) -> erldis_client:sr_scall(Client, <<"flushdb">>).

flushall(Client) -> erldis_client:sr_scall(Client, <<"flushall">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Persistence control commands %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

save(Client) -> erldis_client:scall(Client, <<"save">>).

bgsave(Client) -> erldis_client:scall(Client, <<"bgsave">>).

lastsave(Client) -> erldis_client:scall(Client, <<"lastsave">>).

shutdown(Client) -> erldis_client:scall(Client, <<"shutdown">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Remote server control commands %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

auth(Client, Password) ->
	erldis_client:scall(Client, inline_cmd(<<"auth">>, Password)).

info(Client) -> erldis_client:scall(Client, <<"info">>).

slaveof(Client, Host, Port) ->
	erldis_client:scall(Client, inline_cmd([<<"slaveof">>, Host, Port])).

slaveof(Client) ->
	erldis_client:scall(Client, inline_cmd([<<"slaveof">>, <<"no one">>])).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Multi/Exec Pipelining %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_all_results(Client) -> gen_server2:call(Client, get_all_results).

set_pipelining(Client, Bool) -> gen_server2:cast(Client, {pipelining, Bool}).

exec(Client, Fun) ->
	case erldis_client:sr_scall(Client, <<"multi">>) of
		ok ->
			set_pipelining(Client, true),
			Fun(Client),
			get_all_results(Client),
			set_pipelining(Client, false),
			erldis_client:scall(Client, <<"exec">>);
		_ ->
			{error, unsupported}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%
%% command generators %%
%%%%%%%%%%%%%%%%%%%%%%%%

inline_cmd(Args) -> make_cmd([Args]).

inline_cmd(Cmd, Key) -> inline_cmd([Cmd, Key]).

bulk_cmd(Line1, Bulk) ->
	Bin = erldis_binaries:to_binary(Bulk),
	make_cmd([Line1 ++ [size(Bin)], [Bin]]).

make_cmd(Lines) ->
	BLines = [erldis_binaries:join(Line, <<" ">>) || Line <- Lines],
	erldis_binaries:join(BLines, <<"\r\n">>).

%%%%%%%%%%%%%%%%%%%%%%
%% reply conversion %%
%%%%%%%%%%%%%%%%%%%%%%

numeric(false) -> 0;
numeric(true) -> 1;
numeric(nil) -> 0;
numeric(I) when is_binary(I) -> numeric(binary_to_list(I));
numeric(I) when is_list(I) ->
	try list_to_integer(I)
	catch
		error:badarg ->
			try list_to_float(I)
			catch error:badarg -> I
			end
	end;
numeric(I) -> I.