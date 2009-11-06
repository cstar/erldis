-module(erldis).

-compile(export_all).
-define(EOL, "\r\n").

%% helpers
flatten({error, Message}) ->
    {error, Message};
flatten(List) when is_list(List)->   
    lists:flatten(List).

%% exposed API
connect(Host) ->
    erldis_client:connect(Host).
connect(Host, Port) ->
    erldis_client:connect(Host, Port).
connect(Host, Port, Options) ->
    erldis_client:connect(Host, Port, Options).

quit(Client) ->
    erldis_client:asend(Client, "QUIT"),
    erldis_client:disconnect(Client).

%% Commands operating on string values
internal_set_like(Client, Command, Key, Value) ->
    erldis_client:send(Client, Command, [[Key, length(Value)],
                                  [Value]]).

get_all_results(Client) -> erldis_client:get_all_results(Client).

auth(Client, Password) -> erldis_client:ssend(Client, auth, [Password]).

set(Client, Key, Value) -> internal_set_like(Client, set, Key, Value).
get(Client, Key) -> erldis_client:ssend(Client, get, [Key]).
getset(Client, Key, Value) -> internal_set_like(Client, getset, Key, Value).
mget(Client, Keys) -> erldis_client:ssend(Client, mget, Keys).
setnx(Client, Key, Value) -> internal_set_like(Client, setnx, Key, Value).
incr(Client, Key) -> erldis_client:ssend(Client, incr, [Key]).
incrby(Client, Key, By) -> erldis_client:ssend(Client, incrby, [Key, By]).
decr(Client, Key) -> erldis_client:ssend(Client, decr, [Key]).
decrby(Client, Key, By) -> erldis_client:ssend(Client, decrby, [Key, By]).



%% Commands operating on every value
exists(Client, Key) -> erldis_client:ssend(Client, exists, [Key]).
del(Client, Key) -> erldis_client:ssend(Client, del, [Key]).
type(Client, Key) -> erldis_client:ssend(Client, type, [Key]).
keys(Client, Pattern) -> erldis_client:ssend(Client, keys, [Pattern]).
randomkey(Client, Key) -> erldis_client:ssend(Client, randomkey, [Key]).
rename(Client, OldKey, NewKey) -> erldis_client:ssend(Client, rename, [OldKey, NewKey]).
renamenx(Client, OldKey, NewKey) -> erldis_client:ssend(Client, renamenx, [OldKey, NewKey]).
dbsize(Client) -> erldis_client:ssend(Client, dbsize).
expire(Client, Key, Seconds) -> erldis_client:ssend(Client, expire, [Key, Seconds]).
ttl(Client, Key) -> erldis_client:ssend(Client, ttl, [Key]).



%% Commands operating on lists
rpush(Client, Key, Value) -> internal_set_like(Client, rpush, Key, Value).
lpush(Client, Key, Value) -> internal_set_like(Client, lpush, Key, Value).
llen(Client, Key) -> erldis_client:ssend(Client, llen, [Key]).
lrange(Client, Key, Start, End) -> erldis_client:ssend(Client, lrange, [Key, Start, End]).
ltrim(Client, Key, Start, End) -> erldis_client:ssend(Client, ltrim, [Key, Start, End]).
lindex(Client, Key, Index) -> erldis_client:ssend(Client, lindex, [Key, Index]).
lset(Client, Key, Index, Value) ->
    erldis_client:send(Client, lset, [[Key, Index, length(Value)],
                               [Value]]).
lrem(Client, Key, Number, Value) ->
    erldis_client:send(Client, lrem, [[Key, Number, length(Value)],
                               [Value]]).
lpop(Client, Key) -> erldis_client:ssend(Client, lpop, [Key]).
rpop(Client, Key) -> erldis_client:ssend(Client, rpop, [Key]).



%% Commands operating on sets
sadd(Client, Key, Value) -> internal_set_like(Client, sadd, Key, Value).
srem(Client, Key, Value) -> internal_set_like(Client, srem, Key, Value).
smove(Client, SrcKey, DstKey, Member) -> erldis_client:send(Client, smove, [[SrcKey, DstKey, length(Member)],
                                                                     [Member]]).
scard(Client, Key) -> erldis_client:ssend(Client, scard, [Key]).
sismember(Client, Key, Value) -> internal_set_like(Client, sismember, Key, Value).
sintersect(Client, Keys) -> erldis_client:ssend(Client, sinter, Keys).
sinter(Client, Keys) -> sintersect(Client, Keys).
sinterstore(Client, DstKey, Keys) -> erldis_client:ssend(Client, sinterstore, [DstKey|Keys]).
sunion(Client, Keys) -> erldis_client:ssend(Client, sunion, Keys).
sunionstore(Client, DstKey, Keys) -> erldis_client:ssend(Client, sunionstore, [DstKey|Keys]).
sdiff(Client, Keys) -> erldis_client:ssend(Client, sdiff, Keys).
sdiffstore(Client, DstKey, Keys) -> erldis_client:ssend(Client, sdiffstore, [DstKey|Keys]).
smembers(Client, Key) -> erldis_client:ssend(Client, smembers, [Key]).


%% Multiple DB commands
select(Client, Index) -> erldis_client:ssend(Client, select, [Index]).
move(Client, Key, DBIndex) -> erldis_client:ssend(Client, move, [Key, DBIndex]).
flushdb(Client) -> erldis_client:ssend(Client, flushdb).
flushall(Client) -> erldis_client:ssend(Client, flushall).


%% Commands operating on both lists and sets
sort(Client, Key) -> erldis_client:ssend(Client, sort, [Key]).
sort(Client, Key, Extra) -> erldis_client:ssend(Client, sort, [Key, Extra]).    


%% Persistence control commands
save(Client) -> erldis_client:ssend(Client, save).
bgsave(Client) -> erldis_client:ssend(Client, bgsave).
lastsave(Client) -> erldis_client:ssend(Client, lastsave).
shutdown(Client) -> erldis_client:asend(Client, shutdown).


%% Remote server control commands
info(Client) -> erldis_client:ssend(Client, info).
slaveof(Client, Host, Port) -> erldis_client:ssend(Client, slaveof, [Host, Port]).
slaveof(Client) -> erldis_client:ssend(Client, slaveof, ["no one"]).
