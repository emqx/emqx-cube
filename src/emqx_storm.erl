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
-export([start_link/1]).

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
-spec start_link(map() | list()) ->
                        {ok, Pid :: pid()} |
                        ignore |
                        {error, Error :: term()}.
start_link(Config) when is_list(Config) ->
    start_link(maps:from_list(Config));
start_link(Config) ->
    gen_statem:start_link({local, name(?MODULE)}, ?MODULE, Config, []).

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
init(Config = #{client_id := ClientId}) ->
    process_flag(trap_exit, true),
    Get = fun(K, D) -> maps:get(K, Config, D) end,
    {ok, connecting, Config#{reconnect_delay_ms := Get(reconnect_delay_ms, ?DEFAULT_RECONNECT_DELAY_MS),
                             control_topic => <<"storm/control/", ClientId/binary>>,
                             ack_topic => <<"storm/ack/", ClientId/binary>>}}.

%% @doc Connecting state is a state with timeout.
%% After each timeout, it re-enters this state and start a retry until
%% successfuly connected to remote node/cluster.
connecting(enter, connected, #{reconnect_delay_ms := Timeout}) ->
    Action = {state_timeout, Timeout, reconnect},
    {keep_state_and_data, Action};
connecting(enter, _, #{reconnect_delay_ms := Timeout} = State) ->
    ConnectConfig = maps:without([reconnect_delay_ms], State),
    case connect(ConnectConfig) of
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
            ?LOG(info, "Storm ~p diconnected ~n reason=~p", [name(), ConnPid, Reason]),
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

connect(Config = #{control_topic := Recv,
                   ack_topic  := Snd}) ->
    Ref = make_ref(),
    Parent = self(),
    Handlers = make_msg_handler(Snd, Parent, Ref),
    ConnectConfig = maps:without([control_topic, ack_topic],
                                 Config#{msg_handler => Handlers}),
    Subs = [{Recv, 1}],
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

make_msg_handler(Snd, Parent, Ref) ->
    #{publish => fun(Msg) -> 
                         handle_msg(Msg, #{ack_topic => Snd})
                 end,
      puback => fun(_Ack) -> ok end,
      disconnected => fun(Reason) -> Parent ! {disconnected, Ref, Reason} end}.

handle_msg(#{topic     := ControlTopic,
             payload   := Payload},
           #{control_topic := ControlTopic,
             ack_topic  := RspTopic}) ->
    handle_payload(Payload, RspTopic);
handle_msg(_Msg, _Interaction) ->
    ok.

handle_payload(Payload, RspTopic) ->
    Req = emqx_json:safe_decode(Payload),
    RspPayload = handle_request(Req),
    RspMsg = make_rsp_msg(RspTopic, RspPayload),
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

handle_request(Req) ->
    Type = b2l(get_value(<<"type">>, Req, [])),
    Fun = b2a(get_value(<<"action">>, Req, [])),
    RawArgs = emqx_json:safe_decode(
                get_value(<<"payload">>, Req, [])),
    Args = convert(RawArgs),
    Module = list_to_atom("emqx_storm_" ++ Type),
    {ok, Result} = Module:Fun(Args),
    Rsp = return(maps:from_list(Result)),
    emqx_json:encode(restruct(Rsp, Req)).

b2a(Data) ->
    binary_to_atom(Data, utf8).

b2l(Data) ->
    binary_to_list(Data).

convert(RawArgs) when is_list(RawArgs) ->
    convert(RawArgs, []).

convert([], Acc) ->
    maps:from_list(Acc);
convert([{K, V} | RestProps], Acc) ->
    convert(RestProps, [{b2a(K), V} | Acc]).

return(#{code := Code}) ->
    [{code, Code}, {payload, <<>>}];
return(#{code := Code, data := Data}) when is_map(Data) ->
    [{code, Code}, {payload, maps:to_list(Data)}];
return(#{code := Code, data := Data}) ->
    [{code, Code}, {payload, Data}];
return(_Map) ->
    [{code, 404, {payload, <<"Not found">>}}].

restruct(Resp, Req) ->
    RspKeys = proplists:get_keys(Resp),
    Req1 = delete_by_keys(RspKeys, Req),
    lists:append(Req1, Resp).

delete_by_keys([], Req) ->
    Req;
delete_by_keys([Key | LeftKeys], Req) ->
    delete_by_keys(LeftKeys, delete(Key, Req)).

make_rsp_msg(Topic, Payload) ->
    #mqtt_msg{qos = 1,
              topic = Topic,
              payload = Payload}.
