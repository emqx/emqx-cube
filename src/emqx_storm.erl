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

-type qos() :: emqx_mqtt_types:qos_name() | emqx_mqtt_types:qos().

%% API
-export([start_link/2, stop/1]).

-define(SERVER, ?MODULE).

-spec(start_link(atom(), list() | map()) -> {ok, Pid :: pid()} |
                                    ignore |
                                    {error, Error :: term()}).
start_link(Name, Config) when is_list(Config) ->
    start_link(Name, maps:from_list(Config));
start_link(Name, Config) ->
    gen_statem:start_link({local, name(Name)}, ?MODULE, Config, []).

%% @doc Config should be a map().
init(Config) ->
    erlang:process_flag(trap_exit, true),    
    MsgHandler = make_msg_handler(Parent),
    ConnectConfig = maps:without([], _)


stop(Pid) ->
    emqx_client:disconnect(Pid).

connect(Options) ->
    case Module:start(Config) of
        {ok, Ref, Conn} ->
            {ok, Ref, Conn};
        {error, Reason} ->
            Config1 = obfuscate(Config),
            ?LOG(error, "Failed to connect with module=~p\n"
                 "config=~p\nreason:~p", [Module, Config1, Reason]),
            error
    end.

make_msg_handler(Parent) ->
    #{publish => fun(Msg) -> handle_msg(Msg, Parent) end,
      puback => fun(_Ack) -> ok end,
      disconnected => fun(_Reason) -> ok end}.

handle_msg(Msg, ) ->
    ok.

gen_options(Options) ->
    gen_options(Options, []).
gen_options([], Acc) ->
    Acc;
gen_options([{devicekey, DeviceKey}| Options], Acc) ->
    gen_options(Options, [{client_id, DeviceKey}, {username, DeviceKey} | Acc]);
gen_options([{proto_ver, ProtoVer}| Options], Acc) ->
    gen_options(Options, [{proto_ver, proto_ver(ProtoVer)}|Acc]);
gen_options([{devicesecret, DeviceSecret}| Options], Acc) ->
    gen_options(Options, [{password, DeviceSecret} | Acc]);
gen_options([{keepalive, Keepalive} | Options], Acc) ->
    gen_options(Options, [{keepalive, Keepalive}|Acc]);
gen_options([{clean_start, CleanStart}| Options], Acc) ->
    gen_options(Options, [{clean_start, CleanStart}|Acc]);
gen_options([{address, Address}| Options], Acc) ->
    {Host, Port} = address(Address),
    gen_options(Options, [{host, Host}, {port, Port}|Acc]);
gen_options([{ssl, Ssl}| Options], Acc) ->
    gen_options(Options, [{ssl, Ssl}|Acc]);
gen_options([{ssl_opts, SslOpts}| Options], Acc) ->
    gen_options(Options, [{ssl_opts, SslOpts}|Acc]);
gen_options([_Option | Options], Acc) ->
    gen_options(Options, Acc).    

address(Address) ->
    case string:tokens(Address, ":") of
        [Host] -> {Host, 1883};
        [Host, Port] -> {Host, list_to_integer(Port)}
    end.

proto_ver(mqttv3) -> v3;
proto_ver(mqttv4) -> v4;
proto_ver(mqttv5) -> v5.

name() -> {_, Name} = process_info(self(), registered_name), Name.

name(Id) -> list_to_atom(lists:concat([?MODULE, "_", Id])).

id(Pid) when is_pid(Pid) -> Pid;
id(Name) -> name(Name).
