%% @doc This module is used to access a sharded Redis cluster.
%%
%% An important thing to note about sharding is that if you change the ring,
%% keys will be hashed on different shards. This requires a data migration. Right now 
%% it is recommend to do pre-sharding, which means, create a large number of shards up front,
%% and as your data set grows, move each small shard to its own dedicated box. This means that 
%% you will not have to re-hash keys for a very long time.
%% 
%% See this link for more information: http://antirez.com/post/redis-presharding.html
%%
%% `erldis_shard:start_link([{"redis01", [{{{"127.0.0.1", 6379}, 5}, master}, {{{"127.0.0.1", 6379}, 5}, slave}, {{{"127.0.0.1", 6380}, 5}, slave}]}, {"redis02", [{{{"127.0.0.1", 6379}, 5}, master}]}]),'
%%
%% `erldis:get(erldis_shard:client("mykey", master), "mykey").'
%% 
%% 
%% @type shard_spec() = [shard()]. A shard_spec() contains a list of shard().
%% @type shard() = {string(), [{pool_conn_spec(), atom()}]}. Information about a shard.
%%
%%
-module(erldis_shard).

-export([
    start_link/1,
    client/2,
    get_slot/1,
    get_ring/0
]).

%% This value specifies how many times one item will appear on the ring. 
-define(DEFAULT_NUM_REPLICAS, 128).

%% @doc Initializes a Redis sharded cluster with the given ShardList.
%%
%% ShardSpec contains a mapping of slot -> hosts:
%%
%% `[
%%  {"redis01", [
%%      {{{"127.0.0.1", 6379}, 5}, master},
%%      {{{"127.0.0.1", 6380}, 5}, slave}
%%  ]},
%%  {"redis02", [
%%      {{{"127.0.0.1", 6380}, 1}, master}
%%  }
%%  ...
%% ]'
%%
%% @spec start_link(shard_spec()) -> {ok, pid()}
%%
start_link(ShardSpec) ->
    catch ets:new(?MODULE, [public, named_table, bag]),
    ets:delete_all_objects(?MODULE),
    
    % Create a ring that contains all of the slots
    NumReplicas = case application:get_env(erldis, hash_num_replicas) of
        {ok, Val} -> Val;
        _ -> ?DEFAULT_NUM_REPLICAS
    end,
    SlotNames = lists:map(fun({Name, _}) -> Name end, ShardSpec),
    Ring = hash_ring:create_ring(SlotNames, NumReplicas),
    ets:insert(?MODULE, {ring, Ring}),
    
    % Store the (slot, type) -> hosts mapping in ETS
    lists:foreach(fun({SlotName, Hosts}) ->
        lists:foreach(fun({{HostInfo, _PoolSize}, Type}) ->
            ets:insert(?MODULE, {{SlotName, Type}, HostInfo})
        end, Hosts)
    end, ShardSpec),
    
    % Extract all the hosts from the ShardList
    ConnList = lists:flatten(lists:map(fun({_SlotName, Hosts}) ->
        lists:map(fun({ConnSpec, _}) -> ConnSpec end, Hosts)
    end, ShardSpec)),
    
    % Start the pool supervisor to manage all the connections
    {ok, Pid} = erldis_pool_sup:start_link(ConnList),
    {ok, Pid}.

%%
%% @doc Returns the current Ring
%%
get_ring() ->
    [{ring, Ring}] = ets:lookup(?MODULE, ring),
    Ring.

%%
%% @doc Returns the slot in the ring for the given Key.
%%
%% @spec get_slot(string() | binary()) -> string()
get_slot(Key) ->
    Ring = get_ring(),
    
    % Find the slot that maps to the Key
    hash_ring:get_item(Key, Ring).

%%
%% @doc Returns a client for the shard that contains Key for the given Type.
%%
%% Type is specified in ShardSpec when creating your shards.
%% @see shard_spec()
%%
%% @spec client(any(), atom()) -> undefined | pid()
client(Key, Type) ->
    Slot = get_slot(Key),
    
    % Find a list of hosts for the given slot and type
    Hosts = lists:map(fun({_K, V}) -> V end, ets:lookup(?MODULE, {Slot, Type})),
    case Hosts of
        [] -> undefined;
        _ ->
            % Get a random host of the given type and get a client from the pool
            RandomHost = lists:nth(erlang:phash(now(), length(Hosts)), Hosts),
            erldis_pool_sup:get_random_pid(RandomHost)
    end.