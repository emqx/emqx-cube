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

-module(emqx_cube_datasync).

-define(NO_BRIDGE, undefined).

-include("emqx_cube.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/qlc.hrl").

-import(emqx_cube, [ b2a/1
                    , b2l/1
                    , encode_result/2
                    , make_rsp_msg/2
                    , send_response/2
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
    ok = ekka_mnesia:create_table(?TAB, [{type, set},
                                         {disc_copies, [node()]},
                                         {record_name, ?TAB},
                                         {attributes, record_info(fields, ?TAB)},
                                         {storage_properties,
                                          [{ets, [{read_concurrency, true},
                                                  {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

%%------------------------------------------------------------------------------
%% API functions
%%------------------------------------------------------------------------------

-spec(list(Bindings :: map()) -> {ok, list()}).
list(_Bindings) ->
    all_bridges().

-spec(update(BridgeSpec :: map()) -> {ok, list()}).
update(BridgeSpec = #{id := Id, name := Name}) ->
    emqx_bridge_mqtt_sup:drop_bridge(maybe_b2a(Name)),
    update_bridge(Id, Name, BridgeSpec),
    post_start_bridge(BridgeSpec).

-spec(lookup(BridgeSpec :: map()) -> {ok, list()}).
lookup(#{id := Id}) ->
    {ok, case lookup_bridge(Id) of
             ?NO_BRIDGE ->
                 [{code, ?ERROR4}];
             {_Id, _Name, Options} ->
                 [{code, ?SUCCESS},
                  {data, maps:without([rsp_topic, cube_pid], Options)}]
         end}.

-spec(add(BridgeSpec :: map()) -> {ok, list()}).
add(BridgeSpec = #{id := Id, name := Name}) ->
    ret(add_bridge(Id, Name, maps:remove(cube_pid, BridgeSpec))).

-spec(delete(BridgeSpec :: map()) -> {ok, list()}).
delete(#{id := Id}) ->
    ret(remove_bridge(Id)).

-spec(start(BridgeSpec :: map()) -> {ok, list()}).
start(BridgeSpec) ->
    post_start_bridge(BridgeSpec).

-spec(create(BridgeSpec :: map()) -> {ok, list()}).
create(BridgeSpec = #{id := Id, name := Name}) ->
    case add_bridge(Id, Name, BridgeSpec) of
        {atomic, ok} ->
            post_start_bridge(fun remove_bridge/1, BridgeSpec);
        {aborted, existed} ->
            update(BridgeSpec),
            post_start_bridge(BridgeSpec);
        {aborted, Error} ->
            {ok, [{code, ?ERROR4}, {data, Error}]}
    end.

-spec(stop(map()) -> {ok, list()}).
stop(#{id := Id}) ->
    {ok, stop_bridge(Id)}.

-spec(status(map()) -> {ok, list()}).
status(_Bindings) ->
    bridges_status().

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

update_bridge(Id, Name, Options) ->
    Bridge = #?TAB{id = Id, name = Name, options = Options},
    mnesia:transaction(fun do_update_bridge/1, [Bridge]).

do_update_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [_|_] -> mnesia:write(Bridge);
        [] -> mnesia:abort(noexisted)
    end.

post_start_bridge(BridgeSpec) ->
    post_start_bridge(fun(_Id) -> ok end, BridgeSpec).

post_start_bridge(PostAction, BridgeSpec = #{id := Id}) ->
    case start_bridge(BridgeSpec) of
        failed ->
            PostAction(Id),
            ?LOG(error, "[Cube] Bridge failed to start, Id : ~p", [Id]),
            {ok, [{code, ?ERROR4},
                  {data, <<"Start bridge failed">>}]};
        RetValue ->
            {ok, RetValue}
    end.

all_bridges() -> 
    Bridges = list_bridges(),
    {ok, [{code, ?SUCCESS},
          {data, [Bridge ||{_TabName, _Id, _Name, Bridge} <- Bridges]}]}.

list_bridges() ->
    ets:tab2list(?TAB).

add_bridge(Id, Name, Options) ->
    Config = #?TAB{id = Id, name = Name, options = Options},
    mnesia:transaction(fun insert_bridge/1, [Config]).

insert_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [] -> mnesia:write(Bridge);
        [_ | _] -> mnesia:abort(existed)
    end.

remove_bridge(Id) ->
    handle_lookup(Id, fun(Name, _Options) ->
                          emqx_bridge_mqtt_sup:drop_bridge(maybe_b2a(Name))
                      end),
    mnesia:transaction(fun mnesia:delete/1, [{?TAB, Id}]).

start_bridge(#{ id := Id, rsp_topic := RspTopic, cube_pid := CubePid}) ->
    BridgeHandler = fun(Status) ->
                        {ok, RspPayload}
                                = encode_result([ {code, 200}
                                                , {data, #{ id => Id
                                                          , status => Status}}
                                                ], [ {tid, Id}
                                                   , {type, <<"datasync">>}
                                                   , {action, <<"status">>}]),
                        RspMsg = make_rsp_msg(RspTopic, RspPayload),
                        ok = send_response(CubePid, RspMsg)
                    end,
    StartBridge = fun(Name, Options) ->
                      Name1 = maybe_b2a(Name),
                      Options1 = trans_opts(maps:to_list(Options), Name),
                      emqx_bridge_mqtt_sup:create_bridge(Name1, Options1#{bridge_handler => BridgeHandler}),
                      try emqx_bridge_worker:ensure_started(Name1) of
                          ok -> [{code, ?SUCCESS},
                                 {data, <<"Start bridge successfully">>}];
                          connected -> [{code, ?SUCCESS},
                                        {data, <<"Bridge already started">>}];
                          _ -> failed
                      catch
                          _Error:_Reason -> failed
                      end
                  end,
    handle_lookup(Id, StartBridge).
    
bridges_status() ->
    Bridges = list_bridges(),
    {ok, [{code, ?SUCCESS},
          {data, [get_bridge_status(Name)
                  || {_TabName, _Id, Name, _Bridge} <- Bridges]}]}.

get_bridge_status(Name) ->
    try emqx_bridge_worker:status(Name) of
        standing_by -> disconnected;
        Status -> Status
    catch
        _Error:_Reason -> disconnected
    end.

stop_bridge(Id) ->
    DropBridge = fun(Name, _Options) ->
                     Name1 = maybe_b2a(Name),
                     case emqx_bridge_mqtt_sup:drop_bridge(Name1) of
                         ok -> [{code, ?SUCCESS}, {data, <<"stop bridge successfully">>}];
                         {error, _Error} -> [{code, ?ERROR4}, {data, <<"stop bridge failed">>}]
                     end
                 end,
    handle_lookup(Id, DropBridge).

handle_lookup(Id, Handler) ->
    case lookup_bridge(Id) of
        {_Id, Name, Options} -> Handler(Name, Options);
        _Error -> ?LOG(error, "[Cube] Bridge[~p] not found", [Id]),
                  [{code, ?ERROR4}, {data, <<"bridge_not_found">>}]
    end.

%% @doc Lookup bridge by id
lookup_bridge(Id) ->
    case mnesia:dirty_read(?TAB, Id) of
        [{?TAB, Id, Name, Options}] ->
            {Id, Name, Options};
        _ ->
            ?NO_BRIDGE
    end.

ret({atomic, ok})     -> {ok, [{code, ?SUCCESS}]};
ret({aborted, Error}) -> {ok, [{code, ?ERROR4}, {data, Error}]}.

trans_opts(RawArgs, Name) when is_map(RawArgs) ->
    trans_opts(maps:to_list(RawArgs), Name);
trans_opts(RawArgs, Name) when is_list(RawArgs) ->
    trans_opts(RawArgs, [], Name).

trans_opts([], Acc, _Name) ->
    Acc1 = maps:from_list(Acc),
    Acc1#{ connect_module => emqx_bridge_mqtt};
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
                                _Flag -> cuttlefish_flag:parse(maybe_b2a(SslFlag))
                            end} | Acc], Name);
trans_opts([{ssl_opt, SslOpt} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{ssl_opt, SslOpt} | Acc], Name);
trans_opts([{start_type, StartType} | RestProps], Acc, Name) ->
    trans_opts(RestProps, [{start_type, maybe_b2a(StartType)} | Acc], Name);
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
