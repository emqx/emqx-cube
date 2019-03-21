%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_storm_datasync).

-define(NO_BRIDGE, undefined).

-include("emqx_storm.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/qlc.hrl").

-import(emqx_storm, [ b2a/1
                    , b2l/1
                    ]).

-import(proplists, [ get_value/2 ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-else.
-export([mnesia/1]).
-export([ list/1
        , update/1
        , lookup/1
        , add/1
        , delete/1
        , start/1
        , create/1
        , stop/1
        , status/1
        ]).
-endif.

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).


%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {disc_copies, [node()]},
                {record_name, ?TAB},
                {attributes, record_info(fields, ?TAB)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

list(_Bindings) ->
    all_bridges().

update(BridgeSpec = #{id := Id, name := Name}) ->
    ret(update_bridge(Id, Name, trans_opts(BridgeSpec))).

lookup(#{id := Id}) ->
    {ok, case lookup_bridge(Id) of
             ?NO_BRIDGE ->
                 [{code, ?ERROR4}];
             Bridge ->
                 [{code, ?SUCCESS},
                  {data, Bridge}]
         end}.

add(BridgeSpec = #{id := Id, name := Name}) ->
    ret(add_bridge(Id, Name, trans_opts(BridgeSpec))).

delete(#{id := Id}) ->
    ret(remove_bridge(Id)).

start(#{id := Id}) ->
    start_bridge(Id).

create(BridgeSpec = #{id := Id, name := Name}) ->
    case add_bridge(Id, Name, trans_opts(BridgeSpec)) of
        {atomic, ok} ->
            start_bridge(Id);
        {aborted, Error} ->
            {ok, [{code, ?ERROR4}, {data, Error}]}
    end.

stop(#{id := Id}) ->
    stop_bridge(Id).

status(_Bindings) ->
    bridge_status().

-spec(all_bridges() -> list()).
all_bridges() -> 
    try ets:tab2list(?TAB) of
        Result -> 
            {ok, [{code, ?SUCCESS},
                  {data, Result}]}
    catch
        _Error:_Reason ->
            {ok, [{code, ?ERROR4}]}
    end.

-spec(add_bridge(Id :: atom() | list(),
                 Name :: atom() | list(),
                 Options :: list()) 
      -> {ok, list()}).
add_bridge(Id, Name, Options) ->
    Config = #?TAB{id = Id, name = Name, options = Options},
    mnesia:transaction(fun insert_bridge/1, [Config]).

-spec insert_bridge(Bridge :: ?TAB()) ->  ok | no_return().
insert_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [] -> mnesia:write(Bridge);
        [_ | _] -> mnesia:abort(existed)
    end.

-spec(update_bridge(list(), list(), list()) -> {ok, list()}).
update_bridge(Id, Name, Options) ->
    Bridge = #?TAB{id = Id, name = Name, options = Options},
    mnesia:transaction(fun do_update_bridge/1, [Bridge]).

do_update_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [_|_] -> mnesia:write(Bridge);
        [] -> mnesia:abort(noexisted)
    end.

-spec(remove_bridge(atom()) -> ok | {error, any()}).
remove_bridge(Id) ->
    mnesia:transaction(fun mnesia:delete/1,
                       [{?TAB, Id}]).

-spec(start_bridge(atom()) -> {ok, list()}).
start_bridge(Id) ->
    StartBridge = fun(Name, Options) ->
                          emqx_bridge_sup:create_bridge(Name, Options),
                          try emqx_bridge:ensure_started(Name) of
                              ok -> [{code, ?SUCCESS},
                                     {data, <<"Start bridge successfully">>}];
                              connected -> [{code, ?SUCCESS},
                                            {data, <<"Bridge already started">>}];
                              _ -> [{code, ?ERROR4},
                                    {data, <<"Start bridge failed">>}]
                          catch
                              _Error:_Reason ->
                                  [{code, ?ERROR4},
                                   {data, <<"Start bridge failed">>}]
                          end
                  end,
    {ok, handle_lookup(Id, StartBridge)}.
    
-spec(bridge_status() -> list()).
bridge_status() ->
    {ok, [{code, ?SUCCESS},
          {data, emqx_bridge_sup:bridges()}]}.

-spec(stop_bridge(atom() | list() ) -> ok| {error, any()}).
stop_bridge(Id) ->
    DropBridge = fun(Name, _Options) ->
                     case emqx_bridge_sup:drop_bridge(Name) of
                         ok -> 
                             [{code, ?SUCCESS},
                              {data, <<"stop bridge successfully">>}];
                         _Error ->
                             [{code, ?ERROR4},
                              {data, <<"stop bridge failed">>}]
                     end
                 end,
    {ok, handle_lookup(Id, DropBridge)}.

handle_lookup(Id, Handler) ->
    case lookup_bridge(Id) of
        {Name, Options} ->
            Handler(Name, Options);
        _Error ->
            [{code, ?ERROR4},
             {data, <<"bridge_not_found">>}]
    end.

%% @doc Lookup bridge by id
-spec(lookup_bridge(atom()) -> tuple() | ?NO_BRIDGE).
lookup_bridge(Id) ->
    case mnesia:dirty_read(?TAB, Id) of
        [{?TAB, _Id, Name, Option}] ->
            {Name, Option};
        _ ->
            ?NO_BRIDGE
    end.

-spec(ret(Args :: {atomic, ok} | {aborted, any()})
      -> {ok, list()}).
ret({atomic, ok})     -> {ok, [{code, ?SUCCESS}]};
ret({aborted, Error}) -> {ok, [{code, ?ERROR4}, {data, Error}]}.

trans_opts(RawArgs) when is_map(RawArgs) ->
    trans_opts(maps:to_list(RawArgs));
trans_opts(RawArgs) when is_list(RawArgs) ->
    trans_opts(RawArgs, []).

trans_opts([], Acc) ->
    [{connect_module, emqx_bridge_mqtt} | Acc];
trans_opts([{address, Address} | RestProps], Acc) ->
    trans_opts(RestProps, [{address, binary_to_list(Address)} | Acc]);
trans_opts([{forwards, Forwards} | RestProps], Acc) ->
    NewForwards = lists:map(fun(OldForwards) -> binary_to_list(OldForwards) end, Forwards),
    trans_opts(RestProps, [{forwards, NewForwards} | Acc]);
trans_opts([{keepalive, KeepAlive} | RestProps], Acc) ->
    trans_opts(RestProps, [{keepalive, cuttlefish_duration:parse(b2l(KeepAlive), s)} | Acc]);
trans_opts([{max_inflight_batches, MaxInflightBatches} | RestProps], Acc) ->
    trans_opts(RestProps, [{max_inflight_batches, MaxInflightBatches} | Acc]);
trans_opts([{proto_ver, ProtoVer} | RestProps], Acc) ->
    NewProtoVer = case ProtoVer of
                      <<"mqttv3">> -> v3;
                      <<"mqttv4">> -> v4;
                      <<"mqttv5">> -> v5
                  end,
    trans_opts(RestProps, [{proto_ver, NewProtoVer} | Acc]);
trans_opts([{queue, QueueOpts} | RestProps], Acc) ->
    NewQueueOpts = lists:map(fun({<<"batch_count_limit">>, BatchCountLimit}) ->
                                     {batch_count_limit, 
                                      cuttlefish_duration:parse(b2l(BatchCountLimit), s)};
                                ({<<"batch_bytes_limit">>, BatchBytesLimit}) ->
                                     {batch_bytes_limit, cuttlefish_bytesize:parse(b2l(BatchBytesLimit))};
                                ({<<"replayq_dir">>, ReplayqDir}) ->
                                     {replayq_dir, b2l(ReplayqDir)};
                                ({<<"replayq_seg_bytes">>, ReplaySegBytes}) ->
                                     {replayq_seg_bytes, cuttlefish_bytesize:parse(b2l(ReplaySegBytes))}
                             end, QueueOpts),
    trans_opts(RestProps, [{queue, NewQueueOpts} | Acc]);
trans_opts([{reconnect_interval, ReconnectInterval} | RestProps], Acc) ->
    trans_opts(RestProps, [{reconnect_interval, cuttlefish_duration:parse(b2l(ReconnectInterval))} | Acc]);
trans_opts([{retry_interval, RetryInterval} | RestProps], Acc) ->
    trans_opts(RestProps, [{retry_interval, cuttlefish_duration:parse(b2l(RetryInterval))} | Acc]);
trans_opts([{ssl, SslFlag} | RestProps], Acc) ->
    trans_opts(RestProps, [{ssl, cuttlefish_flag:parse(b2a(SslFlag)) } | Acc]);
trans_opts([{ssl_opt, SslOpt} | RestProps], Acc) ->
    trans_opts(RestProps, [{ssl_opt, SslOpt} | Acc]);
trans_opts([{start_type, StartType} | RestProps], Acc) ->
    trans_opts(RestProps, [{start_type, b2a(StartType)} | Acc]);
trans_opts([{subscriptions, Subscriptions} | RestProps], Acc) ->
    NewSubscriptions = lists:map(fun(Subscription) ->
                                         { b2l(get_value(Subscription, <<"topic">>))
                                         , get_value(Subscription, <<"qos">>)}
                                 end,
                                 Subscriptions),
    trans_opts(RestProps, [NewSubscriptions | Acc]).
