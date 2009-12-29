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

get_all_results(Client) -> gen_server:call(Client, get_all_results).

set_pipelining(Client,Bool)->
   gen_server:cast(Client, {pipelining, Bool}).

quit(Client) ->
    erldis_client:scall(Client, <<"QUIT">>),
    erldis_client:disconnect(Client).

%% Commands operating on string values
internal_set_like(Client, Command, Key, Value) when is_binary(Key) andalso is_binary(Value) ->
  Size = list_to_binary(integer_to_list(size(Value))),
  Space = <<" ">>,
  CR = <<"\r\n">>,
  case erldis_client:call(Client, <<Command/binary,Key/binary,Space/binary,Size/binary,CR/binary,Value/binary>>) of
      [{error, _}=Error]->Error;
      [R] when R == ok orelse R==nil orelse R == true orelse R == false -> R;
      R -> R
    end;
  
internal_set_like(Client, Command, Key, Value) when is_binary(Value)->
    case erldis_client:call(Client, Command, [[Key, size(Value)], [Value]]) of
      [{error, _}=Error]->Error;
      [R] when R == ok orelse R==nil orelse R == true orelse R == false -> R;
      R -> R
    end;
    
internal_set_like(_Client, _Command, _Key, _Value) ->
  {error, invalid_value}.

%get_all_results(Client) -> erldis_client:get_all_results(Client).

auth(Client, Password) -> 
  Verb = <<"auth ">>,
  erldis_client:scall(Client, <<Verb/binary, Password/binary>>).

set(Client, Key, Value) -> internal_set_like(Client, <<"set ">>, Key, Value).

get(Client, Key) -> 
  Verb = <<"get ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).

exec(Client, Fun)->
  case erldis_client:sr_scall(Client, <<"multi ">>) of
  ok ->  
    erldis:set_pipelining(Client,true),
    Fun(Client),
    erldis:get_all_results(Client),
    erldis:set_pipelining(Client,false),
    erldis_client:scall(Client, <<"exec ">>);
  _ ->
    {error, unsupported}
  end.

 
getset(Client, Key, Value) -> internal_set_like(Client, <<"getset ">>, Key, Value).
mget(Client, Keys) -> erldis_client:scall(Client, <<"mget ">>, Keys).
setnx(Client, Key, Value) -> internal_set_like(Client, <<"setnx ">>, Key, Value).
incr(Client, Key) -> 
  Verb =  <<"incr ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).
incrby(Client, Key, By) -> erldis_client:sr_scall(Client, <<"incrby ">>, [Key, By]).
decr(Client, Key) -> 
  Verb =  <<"decr ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).
decrby(Client, Key, By) -> erldis_client:sr_scall(Client, <<"decrby ">>, [Key, By]).



%% Commands operating on every value
exists(Client, Key) ->
  Verb =  <<"exists ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).
del(Client, Key) -> 
  Verb =  <<"del ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).
type(Client, Key) -> 
  Verb = <<"type ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).
keys(Client, Pattern) ->
  Verb =  <<"keys ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Pattern/binary>>).
randomkey(Client, Key) ->
  Verb =  <<"randomkey ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).
rename(Client, OldKey, NewKey) -> 
  Verb =  <<"rename ">>,
  Space = <<" ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, OldKey/binary, Space/binary, NewKey/binary>>).
renamenx(Client, OldKey, NewKey) -> 
  Verb =  <<"renamenx ">>,
  Space = <<" ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, OldKey/binary, Space/binary, NewKey/binary>>).
dbsize(Client) -> erldis_client:sr_scall(Client, <<"dbsize ">>).
expire(Client, Key, Seconds) ->
  Verb =  <<"expire ">>,
  Space = <<" ">>,
  S = list_to_binary(integer_to_list(Seconds)),
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary, Space/binary, S/binary>>).
  
ttl(Client, Key) -> 
  Verb =  <<"ttl ">>,
  erldis_client:sr_scall(Client, <<Verb/binary, Key/binary>>).



%% Commands operating on lists
rpush(Client, Key, Value) -> internal_set_like(Client, <<"rpush ">>, Key, Value).
lpush(Client, Key, Value) -> internal_set_like(Client, <<"lpush ">>, Key, Value).
llen(Client, Key) -> erldis_client:sr_scall(Client, <<"llen ">>, [Key]).
lrange(Client, Key, Start, End) -> erldis_client:scall(Client, <<"lrange ">>, [Key, Start, End]).
ltrim(Client, Key, Start, End) -> erldis_client:scall(Client, <<"ltrim ">>, [Key, Start, End]).
lindex(Client, Key, Index) -> erldis_client:scall(Client, <<"lindex ">>, [Key, Index]).
lset(Client, Key, Index, Value) ->
    erldis_client:send(Client, <<"lset ">>, [[Key, Index, size(Value)],
                               [Value]]).
lrem(Client, Key, Number, Value) ->
    erldis_client:send(Client, <<"lrem ">>, [[Key, Number, size(Value)],
                               [Value]]).
lpop(Client, Key) -> erldis_client:scall(Client, <<"lpop ">>, [Key]).
rpop(Client, Key) -> erldis_client:scall(Client, <<"rpop ">>, [Key]).



%% Commands operating on sets
sadd(Client, Key, Value) -> internal_set_like(Client, <<"sadd ">>, Key, Value).
srem(Client, Key, Value) -> internal_set_like(Client, <<"srem ">>, Key, Value).
smove(Client, SrcKey, DstKey, Member) -> erldis_client:call(Client, <<"smove ">>, [[SrcKey, DstKey, size(Member)],
                                                                     [Member]]).
scard(Client, Key) -> erldis_client:scall(Client, <<"scard ">>, [Key]).
sismember(Client, Key, Value) -> internal_set_like(Client, <<"sismember ">>, Key, Value).
sintersect(Client, Keys) -> erldis_client:scall(Client, <<"sinter ">>, Keys).
sinter(Client, Keys) -> sintersect(Client, Keys).
sinterstore(Client, DstKey, Keys) -> erldis_client:scall(Client, <<"sinterstore ">>, [DstKey|Keys]).
sunion(Client, Keys) -> erldis_client:scall(Client, <<"sunion ">>, Keys).
sunionstore(Client, DstKey, Keys) -> erldis_client:scall(Client, <<"sunionstore ">>, [DstKey|Keys]).
sdiff(Client, Keys) -> erldis_client:scall(Client, <<"sdiff ">>, Keys).
sdiffstore(Client, DstKey, Keys) -> erldis_client:scall(Client, <<"sdiffstore ">>, [DstKey|Keys]).
smembers(Client, Key) -> erldis_client:scall(Client, <<"smembers ">>, [Key]).


%% Multiple DB commands
select(Client, Index) -> erldis_client:scall(Client, <<"select ">>, [Index]).
move(Client, Key, DBIndex) -> erldis_client:scall(Client, <<"move ">>, [Key, DBIndex]).
flushdb(Client) -> erldis_client:scall(Client,<<"flushdb ">>).
flushall(Client) -> erldis_client:scall(Client, <<"flushall ">>).


%% Commands operating on both lists and sets
sort(Client, Key) -> erldis_client:scall(Client, <<"sort ">>, [Key]).
sort(Client, Key, Extra) -> erldis_client:scall(Client, <<"sort ">>, [Key, Extra]).    


%% Persistence control commands
save(Client) -> erldis_client:scall(Client, <<"save ">>).
bgsave(Client) -> erldis_client:scall(Client, <<"bgsave ">>).
lastsave(Client) -> erldis_client:scall(Client, <<"lastsave ">>).
shutdown(Client) -> erldis_client:scall(Client, <<"shutdown ">>).


%% Remote server control commands
info(Client) -> erldis_client:scall(Client, <<"info ">>).
slaveof(Client, Host, Port) -> erldis_client:scall(Client, <<"slaveof ">>, [Host, Port]).
slaveof(Client) -> erldis_client:scall(Client, <<"slaveof ">>, ["no one"]).
