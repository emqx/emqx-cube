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

-module(emqx_storm).

-behaviour(gen_statem).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% API
-import(proplists, [get_value/2, get_value/3]).
-export([start_link/2]).

-export([]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([initialized/3, connected/3]).

-define(SERVER, ?MODULE).

-record(state, {client_pid         :: pid(),
                options            :: list(),
                reconnect_interval :: pos_integer(),
                queue              :: list(),
                forwards           :: list(),
                subscriptions      :: list()}).

-record(mqtt_msg, {qos = ?QOS_0,
                   retain = false, 
                   dup = false,
                   packet_id, 
                   topic, 
                   props, 
                   payload}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_statem process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(atom(), list()) ->
                        {ok, Pid :: pid()} |
                        ignore |
                        {error, Error :: term()}.
start_link(Name, Options) ->
    gen_statem:start_link({local, name(Name)}, ?MODULE, [Options], []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Define the callback_mode() for this callback module.
%% @end
%%--------------------------------------------------------------------
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> state_functions.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  gen_statem:init_result(atom()).
init([Options]) ->
    process_flag(trap_exit, true),
    case get_value(start_type, Options, manual) of
        manual -> ok;
        auto -> erlang:send_after(1000, self(), start)
    end,
    ReconnectInterval = get_value(reconnect_interval, Options, 30000),
    {ok, initialized, #state{queue = [], reconnect_interval = ReconnectInterval}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one function like this for each state name.
%% Whenever a gen_statem receives an event, the function 
%% with the name of the current state (StateName) 
%% is called to handle the event.
%% @end
%%--------------------------------------------------------------------
-spec initialized('enter',
                  OldState :: atom(),
                  Data :: term()) ->
                         gen_statem:state_enter_result('initialized');
                 (gen_statem:event_type(),
                  Msg :: term(),
                  Data :: term()) ->
                         gen_statem:event_handler_result(atom()).
initialized({call, From}, start_storm, State = #state{client_pid = undefined}) ->
    case storm(start, State) of
        {ok, Msg, NewState} ->
            {next_state, connected, NewState, [{reply, From, #{msg => Msg}}]};
        {error, Msg, NewState} ->
            {keep_state, NewState, [{reply, From, #{msg => Msg}}]}
    end;
initialized({call, From}, stop_storm, State) ->
    {keep_state, State, [{reply, From, #{msg => <<"storm not started">>}}]};
initialized({call, From}, status, State) ->
    {keep_state, State, [{reply, From, #{status => <<"Stopped">>}}]};
initialized(EventType, EventContent, State) ->
    handle_event(EventType, EventContent, initialized, State).

-spec connected('enter',
                OldState :: atom(),
                Data :: term()) ->
                       gen_statem:state_enter_result('connected');
               (gen_statem:event_type(),
                Msg :: term(),
                Data :: term()) ->
                       gen_statem:event_handler_result(atom()).
connected({call, From}, status, State) ->
    {keep_state, State, [{reply, From, #{status => <<"Running">>}}]};
connected({call, From}, stop_bridge, State = #state{client_pid = Pid}) ->
    emqx_client:disconnect(Pid),
    {next_state, initialized, State#state{ client_pid = undefined },
     [{reply, From, #{msg => <<"stop storm successfully">>}}]};
connected(info, {publish, #{qos := QoS, dup := Dup, retain := Retain, 
                            topic := Topic, properties := Props, 
                            payload := Payload}}, State) ->
    NewMsg0 = emqx_message:make(bridge, QoS, Topic, Payload),
    NewMsg1 = emqx_message:set_headers(Props, emqx_message:set_flags(#{dup => Dup, retain => Retain}, NewMsg0)),
    emqx_broker:publish(NewMsg1),
    {keep_state, State};
connected(info, {dispatch, _, #message{topic = Topic, payload = Payload}}, State) ->

    ;
connected(info, {puback, #{packet_id := PktId}}, State) ->
    {keep_state, delete(PktId, State)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), State :: term(), Data :: term()) ->
                       any().
terminate(_Reason, _State, _Data) ->
    void.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(
        OldVsn :: term() | {down,term()},
        State :: term(), Data :: term(), Extra :: term()) ->
                         {ok, NewState :: term(), NewData :: term()} |
                         (Reason :: term()).
code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

-spec handle_event(gen_statem:event_type(), term(), gen_statem:state(), term()) -> 
                          gen_statem:event_handler_result(gen_statem:state()).
handle_event(info, {'EXIT', _Pid, normal}, _StateName, State) ->
    emqx_logger:warning("[Storm] stop ~p", [normal]),
    {next_state, initialized, State#state{ client_pid = undefined }};
handle_event(info, {'EXIT', Pid, Reason}, _StateName, State = #state{client_pid = Pid,
                                                                     reconnect_interval = ReconnectInterval}) ->
    emqx_logger:error("[Storm] stop ~p", [Reason]),
    erlang:send_after(ReconnectInterval, self(), restart),
    {noreply, State#state{client_pid = undefined}};
handle_event(EventType, EventContent, StateName, StateData) ->
    emqx_logger:error("[Storm] State: ~s, Unexpected Event: (~p, ~p)", 
                      [StateName, EventType, EventContent]),
    {keep_state, StateData}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
storm(Action, State = #state{options = Options}) ->
    case emqx_client:start_link([{owner, self()} | options(Options)]) of
        {ok, ClientPid} ->
            case emqx_client:connect(ClientPid) of
                {ok, _} ->
                    emqx_logger:info("[Storm] connected to remote successfully"),
                    {ok, <<"start bridge successfully">>,
                     State#state{client_pid = ClientPid}};
                {error, Reason} ->
                    emqx_logger:error("[Storm] connect to remote failed! error: ~p", [Reason]),
                    {error, <<"connect to remote failed">>,
                     State#state{client_pid = ClientPid}}
            end;
        {error, Reason} ->
            emqx_logger:error("[Storm] ~p failed! error: ~p", [Action, Reason]),
            {<<"start bridge failed">>, State}
    end.
        
proto_ver(mqttv3) -> v3;
proto_ver(mqttv4) -> v4;
proto_ver(mqttv5) -> v5.

address(Address) ->
    case string:tokens(Address, ":") of
        [Host] -> {Host, 1883};
        [Host, Port] -> {Host, list_to_integer(Port)}
    end.

options(Options) ->
    options(Options, []).
options([], Acc) ->
    Acc;
options([{devicekey, DeviceKey}| Options], Acc) ->
    options(Options, [{client_id, DeviceKey}, {username, DeviceKey} | Acc]);
options([{proto_ver, ProtoVer}| Options], Acc) ->
    options(Options, [{proto_ver, proto_ver(ProtoVer)}|Acc]);
options([{devicesecret, DeviceSecret}| Options], Acc) ->
    options(Options, [{password, DeviceSecret} | Acc]);
options([{keepalive, Keepalive} | Options], Acc) ->
    options(Options, [{keepalive, Keepalive}|Acc]);
options([{clean_start, CleanStart}| Options], Acc) ->
    options(Options, [{clean_start, CleanStart}|Acc]);
options([{address, Address}| Options], Acc) ->
    {Host, Port} = address(Address),
    options(Options, [{host, Host}, {port, Port}|Acc]);
options([{ssl, Ssl}| Options], Acc) ->
    options(Options, [{ssl, Ssl}|Acc]);
options([{ssl_opts, SslOpts}| Options], Acc) ->
    options(Options, [{ssl_opts, SslOpts}|Acc]);
options([_Option | Options], Acc) ->
    options(Options, Acc).

name(Id) ->
    list_to_atom(lists:concat([?MODULE, "_", Id])).

delete(PktId, Queue) ->
    lists:keydelete(PktId, 1, Queue).

%% bin(L) -> iolist_to_binary(L).
