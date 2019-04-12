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
                    , encode_result/2
                    , make_rsp_msg/2
                    , send_response/1
                    ]).

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
    emqx_bridge_sup:drop_bridge(maybe_b2a(Id)),
    update_bridge(Id, Name, BridgeSpec),
    create(BridgeSpec).

-spec(update_bridge(list(), list(), list()) -> {ok, list()}).
update_bridge(Id, Name, Options) ->
    Bridge = #?TAB{id = Id, name = Name, options = Options},
    mnesia:transaction(fun do_update_bridge/1, [Bridge]).

do_update_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [_|_] -> mnesia:write(Bridge);
        [] -> mnesia:abort(noexisted)
    end.

lookup(#{id := Id}) ->
    {ok, case lookup_bridge(Id) of
             ?NO_BRIDGE ->
                 [{code, ?ERROR4}];
             {_Id, _Name, Options} ->
                 [{code, ?SUCCESS},
                  {data, maps:remove(rsp_topic, Options)}]
         end}.

add(BridgeSpec = #{id := Id, name := Name}) ->
    ret(add_bridge(Id, Name, BridgeSpec)).

delete(#{id := Id}) ->
    ret(remove_bridge(Id)).

start(BridgeSpec) ->
    post_start_bridge(fun(_Id) -> ok end, BridgeSpec).

post_start_bridge(PostAction, BridgeSpec = #{id := Id}) ->
    case start_bridge(BridgeSpec) of
        {ok, failed} ->
            PostAction(Id),
            ?LOG(error, "Start bridge: ~p failed", [Id]),
            {ok, [{code, ?ERROR4},
                  {data, <<"Start bridge failed">>}]};
        RetValue ->
            RetValue
    end.

create(BridgeSpec = #{id := Id, name := Name}) ->
    case add_bridge(Id, Name, BridgeSpec) of
        {atomic, ok} ->
            post_start_bridge(fun remove_bridge/1, BridgeSpec);
        {aborted, existed} ->
            update(BridgeSpec),
            post_start_bridge(fun(_Id) -> ok end, BridgeSpec);
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
        Bridges ->
            {ok, [{code, ?SUCCESS},
                  {data, [Bridge ||{_TabName, _Id, _Name, Bridge} <- Bridges]}]}
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

-spec(remove_bridge(atom()) -> ok | {error, any()}).
remove_bridge(Id) ->
    emqx_bridge_sup:drop_bridge(maybe_b2a(Id)),
    mnesia:transaction(fun mnesia:delete/1, [{?TAB, Id}]).

-spec(start_bridge(map()) -> {ok, list()}).
start_bridge(#{ id := Id
              , rsp_topic := RspTopic}) ->
    BridgeHandler = fun(Status) ->
                        {ok, RspPayload}
                                = encode_result([ {tid, Id}
                                                , {type, <<"datasync">>}
                                                , {action, <<"status">>}
                                                , {code, 200}
                                                , {data,
                                                   #{ id => Id
                                                    , status => Status}}
                                                ], []),
                        RspMsg = make_rsp_msg(RspTopic, RspPayload),
                        ok = send_response(RspMsg)
                    end,
    StartBridge = fun(Name, Options) ->
                      Name1 = maybe_b2a(Name),
                      Options1 = trans_opts(maps:to_list(Options), Name),
                      emqx_bridge_sup:create_bridge(Name1, Options1#{bridge_handler => BridgeHandler}),
                      try emqx_bridge:ensure_started(Name1) of
                          ok -> [{code, ?SUCCESS},
                                 {data, <<"Start bridge successfully">>}];
                          connected -> [{code, ?SUCCESS},
                                        {data, <<"Bridge already started">>}];
                          _ ->
                              failed

                      catch
                          _Error:_Reason ->
                              failed
                      end
                  end,
    {ok, handle_lookup(Id, StartBridge)}.
    
-spec(bridge_status() -> list()).
bridge_status() ->
    BridgesStatus = [[{id, Id},
                      {status, case Status of
                                   standing_by -> disconnected;
                                   Status0 -> Status0
                               end}]
                     || {Id, Status} <- emqx_bridge_sup:bridges()],
    {ok, [{code, ?SUCCESS},
          {data, BridgesStatus}]}.

-spec(stop_bridge(atom() | list() ) -> ok| {error, any()}).
stop_bridge(Id) ->
    DropBridge = fun(Name, _Options) ->
                     Name1 = maybe_b2a(Name),
                     case emqx_bridge:ensure_stopped(Name1) of
                         ok ->
                             [{code, ?SUCCESS},
                              {data, <<"stop bridge successfully">>}];
                         {error, _Error} ->
                             [{code, ?ERROR4},
                              {data, <<"stop bridge failed">>}]
                     end
                 end,
    {ok, handle_lookup(Id, DropBridge)}.

handle_lookup(Id, Handler) ->
    case lookup_bridge(Id) of
        {_Id, Name, Options} ->
            Handler(Name, Options);
        _Error ->
            ?LOG(error, "Bridge[~p] not found", [Id]),
            [{code, ?ERROR4},
             {data, <<"bridge_not_found">>}]
    end.

%% @doc Lookup bridge by id
-spec(lookup_bridge(atom()) -> tuple() | ?NO_BRIDGE).
lookup_bridge(Id) ->
    case mnesia:dirty_read(?TAB, Id) of
        [{?TAB, Id, Name, Options}] ->
            {Id, Name, Options};
        _ ->
            ?NO_BRIDGE
    end.

-spec(ret(Args :: {atomic, ok} | {aborted, any()})
      -> {ok, list()}).
ret({atomic, ok})     -> {ok, [{code, ?SUCCESS}]};
ret({aborted, Error}) -> {ok, [{code, ?ERROR4}, {data, Error}]}.

trans_opts(RawArgs, Name) when is_map(RawArgs) ->
    trans_opts(maps:to_list(RawArgs), Name);
trans_opts(RawArgs, Name) when is_list(RawArgs) ->
    trans_opts(RawArgs, [], Name).

trans_opts([], Acc, _Name) ->
    maps:from_list([{connect_module, emqx_bridge_mqtt} | Acc]);
trans_opts([{address, Address} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{address, binary_to_list(Address)} | Acc], Name);
trans_opts([{forwards, Forwards} | RestProps], Acc, Name) ->
    NewForwards = lists:map(fun(OldForwards) -> binary_to_list(OldForwards) end, Forwards),
    trans_opts(RestProps, [{forwards, NewForwards} | Acc], Name);
trans_opts([{keepalive, KeepAlive} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{keepalive, cuttlefish_duration:parse(b2l(KeepAlive), s)} | Acc], Name);
trans_opts([{max_inflight_batches, MaxInflightBatches} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{max_inflight_batches, MaxInflightBatches} | Acc], Name);
trans_opts([{proto_ver, ProtoVer} | RestProps], Acc, Name) ->
    NewProtoVer = case ProtoVer of
                      <<"mqttv3">> -> v3;
                      <<"mqttv4">> -> v4;
                      <<"mqttv5">> -> v5
                  end,
    trans_opts(RestProps, [{proto_ver, NewProtoVer} | Acc], Name);
trans_opts([{queue, QueueOpts} | RestProps], Acc, Name) ->
    NewQueueOpts = lists:map(fun({<<"batch_count_limit">>, BatchCountLimit}) ->
                                     {batch_count_limit, maybe_b2i(BatchCountLimit)};
                                ({<<"batch_bytes_limit">>, BatchBytesLimit}) ->
                                     {batch_bytes_limit, cuttlefish_bytesize:parse(b2l(BatchBytesLimit))};
                                ({<<"replayq_dir">>, ReplayqDir}) ->
                                     ReplayqDir1 = <<ReplayqDir/binary, Name/binary>>,
                                     {replayq_dir, b2l(ReplayqDir1)};
                                ({<<"replayq_seg_bytes">>, ReplaySegBytes}) ->
                                     {replayq_seg_bytes, cuttlefish_bytesize:parse(b2l(ReplaySegBytes))}
                             end, QueueOpts),
    trans_opts(RestProps, [{queue, maps:from_list(NewQueueOpts)} | Acc], Name);
trans_opts([{reconnect_interval, ReconnectInterval} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{reconnect_interval, cuttlefish_duration:parse(b2l(ReconnectInterval))} | Acc], Name);
trans_opts([{retry_interval, RetryInterval} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{retry_interval, cuttlefish_duration:parse(b2l(RetryInterval))} | Acc], Name);
trans_opts([{ssl, SslFlag} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{ssl,
                            case SslFlag of
                                Bool when is_boolean(Bool) -> Bool;
                                _Flag -> cuttlefish_flag:parse(b2a(SslFlag))
                            end} | Acc], Name);
trans_opts([{ssl_opt, SslOpt} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{ssl_opt, SslOpt} | Acc], Name);
trans_opts([{start_type, StartType} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{start_type, b2a(StartType)} | Acc], Name);
trans_opts([{subscriptions, Subscriptions} | RestProps], Acc, Name) ->
    NewSubscriptions = lists:map(fun(Subscription) ->
                                         Topic = proplists:get_value(<<"topic">>, Subscription),
                                         QoS = proplists:get_value(<<"qos">>, Subscription),
                                         {b2l(Topic), QoS}
                                 end,
                                 Subscriptions),
    trans_opts(RestProps, [{subscriptions, NewSubscriptions} | Acc], Name);
trans_opts([{id, _Id} | RestProps], Acc, Name) ->
    trans_opts(RestProps, Acc, Name);
trans_opts([{name, _Name} | RestProps], Acc, Name) ->
    trans_opts(RestProps, Acc, Name);
trans_opts([Prop | RestProps], Acc, Name) ->
    trans_opts(RestProps, [Prop | Acc], Name).

maybe_b2a(Value) when is_binary(Value) ->
    b2a(Value);
maybe_b2a(Value) ->
    Value.

maybe_b2i(Value) ->
    try binary_to_integer(Value)
    catch _Error:_Reason -> 32
    end.
