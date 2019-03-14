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

-include("emqx_storm.hrl").
-include_lib("emqx/include/emqx_client.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_RECONNECT_DELAY_MS, timer:seconds(5)).

%% API
-export([start_link/2]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).

-export([connecting/3,
         connected/3]).

-import(proplists, [get_value/3, delete/2]).

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
-spec start_link(atom(), map() | list()) ->
                        {ok, Pid :: pid()} |
                        ignore |
                        {error, Error :: term()}.
start_link(Name, Config) when is_list(Config) ->
    start_link(Name, maps:from_list(Config));
start_link(Name, Config) ->
    gen_statem:start_link({local, name(Name)}, ?MODULE, Config, []).

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
callback_mode() -> [state_functions, state_enter].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Start storm function
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  gen_statem:init_result(atom()).
init(Config) ->
    process_flag(trap_exit, true),
    GetD = fun(K, D) -> maps:get(K, Config, D) end,
    Get = fun(K) -> maps:get(K, Config) end,
    ClientId = list_to_binary(Get(client_id)),
    DataSync = #{recv => <<"sys/control/", ClientId/binary>>,
                 snd => <<"sys/ack/", ClientId/binary>>},
    Sys = #{recv => <<"sys/control/", ClientId/binary>>,
            snd => <<"sys/ack/", ClientId/binary>>},
    ConnectConfig = maps:without([reconnect_delay_ms], 
                                 Config#{datasync => DataSync,
                                         sys => Sys}),
    ConnectFun = fun() -> connect(ConnectConfig) end,
    {ok, connecting, #{connect_fun => ConnectFun,
                       reconnect_delay_ms => GetD(reconnect_delay_ms, 
                                                 ?DEFAULT_RECONNECT_DELAY_MS)}}.

%% @doc Connecting state is a state with timeout.
%% After each timeout, it re-enters this state and start a retry until
%% successfuly connected to remote node/cluster.
connecting(enter, connected, #{reconnect_delay_ms := Timeout}) ->
    Action = {state_timeout, Timeout, reconnect},
    {keep_state_and_data, Action};
connecting(enter, _, #{reconnect_delay_ms := Timeout,
                       connect_fun := ConnectFun} = State) ->
    case ConnectFun() of
        {ok, ConnRef, ConnPid} ->
            ?LOG(info, "Storm ~p connected", [name()]),
            Action = {state_timeout, 0, connected},
            {keep_state, State#{conn_ref => ConnRef, conn_pid => ConnPid}, Action};
        error ->
            Action = {state_timeout, Timeout, reconnect},
            {keep_state_and_data, Action}
    end;
connecting(state_timeout, connected, State) ->
    {next_state, connected, State};
connecting(state_timeout, reconnect, _State) ->
    repeat_state_and_data;
connecting(info, {disconnected, _Ref, _Reason}, _State) ->
    keep_state_and_data;
connecting(Type, Content, State) ->
    common(connecting, Type, Content, State).

connected(enter, _OldState, _State) ->
    keep_state_and_data;
connected(info, {disconnected, ConnRef, Reason},
          #{conn_ref := ConnRefCurrent,
            conn_pid := ConnPid} = State) ->
    case ConnRefCurrent =:= ConnRef of
        true ->
            ?LOG(info, "Storm ~p diconnected~nreason=~p", [name(), ConnPid, Reason]),
            {next_state, connecting,
             State#{conn_ref := undefined, connection := undefined}};
        false ->
            keep_state_and_data
    end;
connected(Type, Content, State) ->
    common(connected, Type, Content, State).

common(StateName, Type, Content, State) ->
    ?LOG(info, "Storm ~p discarded ~p type event at state ~p:\n~p",
         [name(), Type, StateName, Content]),
    {keep_state, State}.

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect(Config = #{datasync  := DataSync, 
                   sys       := Sys}) ->
    Ref = make_ref(),
    Parent = self(),
    Handlers = make_msg_handler(DataSync, Sys, Parent, Ref),
    GetS = fun(K, V) -> maps:get(K, V) end,
    Subs = [{GetS(recv, DataSync), 1},
            {GetS(recv, Sys), 1}],
    ConnectConfig = maps:without([datasync, sys], 
                                 Config#{subscriptions => Subs,
                                         msg_handler => Handlers}),
    case emqx_client:start_link(ConnectConfig) of
        {ok, Pid} ->
            case emqx_client:connect(Pid) of
                {ok, _} ->
                    try
                        subscribe_remote_topics(Pid, Subs),
                        {ok, Ref, Pid}
                    catch
                        throw : Reason ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Error -> Error
    end.

name() -> {_, Name} = process_info(self(), registered_name), Name.

name(Id) -> list_to_atom(lists:concat([?MODULE, "_", Id])).

make_msg_handler(DataSync, Sys, Parent, Ref) ->
    #{publish => fun(Msg) -> 
                         handle_msg(Msg, #{datasync  => DataSync,
                                           sys       => Sys}) 
                 end,
      puback => fun(_Ack) -> ok end,
      disconnected => fun(Reason) -> Parent ! {disconnected, Ref, Reason} end}.

handle_msg(#{topic     := DataSyncTopic, 
             payload   := Payload},
           #{datasync := #{recv := DataSyncTopic,
                           send := RspTopic}}) ->
    handle_payload("datasync", DataSyncTopic, Payload, RspTopic);
handle_msg(#{topic     := SysTopic,
             payload   := Payload},
           #{sys := #{recv := SysTopic,
                      send := RspTopic}}) ->
    handle_payload("sys", SysTopic, Payload, RspTopic);
handle_msg(_Msg, _Interaction) ->
    ok.

handle_payload(Type, ControlTopic, Payload, RspTopic) ->
    ClientId = get_clientid(ControlTopic),
    Req = emqx_json:safe_decode(Payload),
    RspPayload = handle_request(Type, Req, ClientId),
    RspMsg = make_resp_msg(RspTopic, RspPayload),
    ok = send_response(RspMsg).

subscribe_remote_topics(ClientPid, Subscriptions) ->
    lists:foreach(fun({Topic, QoS}) ->
                          case emqx_client:subscribe(ClientPid, Topic, QoS) of
                              {ok, _, _} -> ok;
                              Error -> throw(Error)
                          end
                  end, Subscriptions).

send_response(Msg) ->
    %% This function is evaluated by emqx_client itself.
    %% hence delegate to another temp process for the loopback gen_statem call.
    Client = self(),
    _ = spawn_link(fun() ->
                           case emqx_client:publish(Client, Msg) of
                               ok -> ok;
                               {ok, _} -> ok;
                               {error, Reason} -> exit({failed_to_publish_response, Reason})
                           end
                   end),
    ok.

handle_request(Type, Req, ClientId) ->
    Fun = b2a(get_value(<<"action">>, Req, [])),
    RawArgs = emqx_json:safe_encode(
                get_value(<<"payload">>, Req, []), 
                [return_maps]),
    Args = convert(RawArgs),
    Module = list_to_atom("emqx_storm_" ++ Type),
    {ok, Result} = Module:Fun(Args),
    Resp = return(maps:from_list(Result)),
    restruct(Resp, Req, ClientId).

b2a(Data) ->
    binary_to_atom(Data, utf8).

convert(RawArgs) when is_list(RawArgs) ->
    convert(RawArgs, []).

convert([], Acc) ->
    maps:from_list(Acc);
convert([{K, V} | RestProps], Acc) ->
    convert(RestProps, [{b2a(K), V} | Acc]).

return(#{code := Code}) ->
    [{code, Code}, {payload, <<>>}];
return(#{code := Code, data := Data}) ->
    [{code, Code}, {payload, Data}];
return(#{code := Code, message := Message}) ->
    [{code, Code}, {payload, Message}];
return(#{code := Code, data := Data, meta := Meta}) ->
    [{code, Code}, {payload, Data}, {meta, Meta}].

restruct(Resp, Req, ClientId) ->
    RespKeys = proplists:get_keys(Resp),
    Req1 = delete_by_keys(RespKeys, Req),
    Req1 ++ Resp ++ [{client_id, ClientId}].

delete_by_keys([], Req) ->
    Req;
delete_by_keys([Key | LeftKeys], Req) ->
    delete_by_keys(LeftKeys, delete(Key, Req)).

make_resp_msg(Topic, Payload) ->
    #mqtt_msg{qos = 1,
              topic = Topic,
              payload = Payload}.

get_clientid(Topic) ->
    Tokens = emqx_topic:tokens(Topic),
    lists:last(Tokens).
