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

-module(emqx_storm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BRIDGE1, {bridge1_id, bridge1_name, [{address, "127.0.0.1"}]}).
-define(BRIDGE2, {bridge2_id, bridge2_name, [{address, "127.0.0.2"}]}).
-define(BRIDGE3, {bridge3_id, bridge3_name, [{address, "127.0.0.3"}]}).

-define(CONTROL, <<"storm/control/qg3rgewt135">>).
-define(ACK, <<"storm/ack/qg3rgewt135">>).

-define(BRIDGES, [?BRIDGE1, ?BRIDGE2, ?BRIDGE3]).

all() ->
    [{group, emqx_storm}].

groups() ->
    [{emqx_storm, [sequence],
      [ datasync_t
      , storm_t
      , sys_t
      ]}].

init_per_suite(Config) ->
    application:load(emqx_storm),
    [start_apps(App, SchemaFile, ConfigFile) ||
        {App, SchemaFile, ConfigFile}
            <- [{emqx, deps_path(emqx, "priv/emqx.schema"),
                       deps_path(emqx, "etc/emqx.conf")},
                {emqx_management, deps_path(emqx_management, "priv/emqx_management.schema"),
                                  deps_path(emqx_management, "etc/emqx_management.conf")},
                {emqx_storm, local_path("priv/emqx_storm.schema"),
                             local_path("etc/emqx_storm.conf")}]],
    Config.

end_per_suite(_Config) ->
    mnesia:delete_table(bridges),
    [application:stop(App) || App <- [emqx_storm, emqx_management, emqx]].

storm_t(_Config) ->
    test_sys(),
    test_datasync(),
    ok.

receive_response() ->
    receive
        Msg = {publish, #{topic := ?ACK}} ->
            ct:log("~p", [Msg]),
            true;
        _OtherMsg ->
            receive_response()
    after 100 ->
            false
    end.

test_sys() ->
    {ok, C} = emqx_client:start_link(),
    {ok, _} = emqx_client:connect(C),
    {ok, _, [1]} = emqx_client:subscribe(C, ?ACK, qos1),
    {ok, _} = emqx_client:publish(C, ?CONTROL, construct(sys, <<"nodes">>, <<>>), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(C, ?CONTROL, construct(sys, <<"stats">>, <<>>), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(C, ?CONTROL, construct(sys, <<"metrics">>, <<>>), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(
                               sys, <<"connections">>, 
                               [{'_page', 1},
                                {'_limit', 15}]), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(
                               sys, <<"sessions">>, 
                               [{'_page', 1},
                                {'_limit', 15}]), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(
                               sys, <<"topics">>, 
                               [{'_page', 1},
                                {'_limit', 15}]), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(
                               sys, <<"subscriptions">>, 
                               [{'_page', 1},
                                {'_limit', 15}]), 1),
    ?assert(receive_response()),
    ok = emqx_client:disconnect(C).

test_datasync() ->
    {ok, C} = emqx_client:start_link(),
    {ok, _} = emqx_client:connect(C),
    {ok, _, [1]} = emqx_client:subscribe(C, ?ACK, qos1),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"list">>,
                                       bridge_params()), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"add">>,
                                       bridge_params()), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"lookup">>,
                                       [{id, <<"bridge_id">>}]), 1),
    ?assert(receive_response()),
    %% {ok, _} = emqx_client:publish(
    %%             C, ?CONTROL, construct(datasync, <<"update">>,
    %%                                    bridge_params(<<"bridge_name2">>)), 1),
    %% ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"lookup">>,
                                       [{id, <<"bridge_id">>}]), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"start">>,
                                       [{id, <<"bridge_id">>}]), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"status">>,
                                       <<>>), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"stop">>,
                                       [{id, <<"bridge_id">>}]), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(
                C, ?CONTROL, construct(datasync, <<"delete">>,
                                       [{id, <<"bridge_id">>}]), 1),
    ?assert(receive_response()),
    ok = emqx_client:disconnect(C).


construct(Type, Action, Payload) ->
    TupleList = [{tid, <<"111">>},
                 {type, erlang:atom_to_binary(Type, utf8)},
                 {action, Action},
                 {payload, Payload}],
    emqx_json:encode(TupleList).

bridge_params() ->
    bridge_params(<<"bridge_name">>).
bridge_params(BridgeName) ->
    [{<<"id">>, <<"bridge_id">>},
     {<<"name">>, BridgeName},
     {<<"address">>,<<"127.0.0.1:1883">>},
     {<<"clean_start">>,true},
     {<<"client_id">>,<<"bridge_aws">>},
     {<<"username">>,<<"user">>},
     {<<"forwards">>,[<<"topic1/#">>,<<"topic2/#">>]},
     {<<"keepalive">>,<<"60s">>},
     {<<"max_inflight_batches">>,32},
     {<<"mountpoint">>,<<"bridge/aws/${node}/">>},
     {<<"password">>,<<"passwd">>},
     {<<"proto_ver">>,<<"mqttv4">>},
     {<<"queue">>,
      [{<<"batch_count_limit">>,32},
       {<<"batch_bytes_limit">>,<<"1000MB">>},
       {<<"replayq_dir">>,<<"data/emqx_bridge/">>},
       {<<"replayq_seg_bytes">>,<<"10MB">>}]},
     {<<"reconnect_interval">>,<<"30s">>},
     {<<"retry_interval">>,<<"20s">>},
     {<<"ssl">>,<<"off">>},
     {<<"ssl_opt">>,
      [{<<"cacertfile">>,<<"etc/certs/cacert.pem">>},
       {<<"certfile">>,<<"etc/certs/client-cert.pem">>},
       {<<"keyfile">>,<<"etc/certs/client-key.pem">>},
       {<<"ciphers">>,
        <<"ECDHE-ECDSA-AES256-GCM-SHA384,ECDHE-RSA-AES256-GCM-SHA384">>},
       {<<"psk_ciphers">>,
        <<"PSK-AES128-CBC-SHA,PSK-AES256-CBC-SHA,PSK-3DES-EDE-CBC-SHA,PSK-RC4-SHA">>},
       {<<"tls_versions">>,<<"tlsv1.2,tlsv1.1,tlsv1">>}]},
     {<<"start_type">>,<<"manual">>},
     {<<"subscriptions">>,
      [[{<<"topic">>,<<"topic/1">>},{<<"qos">>,1}],
       [{<<"topic">>,<<"topic/2">>},{<<"qos">>,1}]]}].

sys_t(_Config) ->
    Param = #{node => node()},
    lists:foreach(fun(Value) ->
                      assertmatch_sys_t(Value)
                  end,
                  [emqx_storm_sys:nodes(Param),
                   emqx_storm_sys:stats(Param),
                   emqx_storm_sys:metrics(Param),
                   {meta, emqx_storm_sys:connections(Param#{'_page' => <<"1">>, '_limit' => <<"15">>})},
                   {meta, emqx_storm_sys:sessions(Param#{'_page' => <<"1">>, '_limit' => <<"15">>})},
                   {meta, emqx_storm_sys:topics(Param#{'_page' => <<"1">>, '_limit' => <<"20">>})},
                   {meta, emqx_storm_sys:subscriptions(Param#{'_page' => <<"1">>, '_limit' => <<"15">>})}]),
    ok.

assertmatch_sys_t({meta, Value}) ->
    ?assertMatch({ok, [{code, 0}, {data, _Data}, {meta, _Meta}]}, Value);
assertmatch_sys_t(Value) ->
    ?assertMatch({ok, [{code, 0}, {data, _Data}]}, Value).

datasync_t(_Config) ->
    Parse= fun({ok, ValueList}) ->
               proplists:get_value(data, ValueList)
           end,
    ?assertEqual([], Parse(emqx_storm_datasync:list(#{}))),
    lists:foreach(fun({Id, Name, Options}) ->
                      emqx_storm_datasync:add_bridge(Id, Name, Options)
                  end, ?BRIDGES),
    ct:log("All bridges: ~p", [ets:tab2list(bridges)]),
    ?assertEqual(3, length(Parse(emqx_storm_datasync:all_bridges()))),
    emqx_storm_datasync:delete(#{id => bridge3_id}),
    ?assertEqual(2, length(Parse(emqx_storm_datasync:list(#{})))),
    emqx_storm_datasync:add_bridge(test_id, test_name, bridge_params()),
    ?assertEqual([], Parse(emqx_storm_datasync:status(#{}))),
    ok.

start_apps(App, SchemaFile, ConfigFile) ->
    read_schema_configs(App, SchemaFile, ConfigFile),
    set_special_configs(App),
    application:ensure_all_started(App).

deps_path(App, RelativePath) ->
    %% Note: not lib_dir because etc dir is not sym-link-ed to _build dir
    %% but priv dir is
    Path0 = code:priv_dir(App),
    Path = case file:read_link(Path0) of
               {ok, Resolved} -> Resolved;
               {error, _Config} -> Path0
           end,
    filename:join([Path, "..", RelativePath]).

local_path(RelativePath) ->
    deps_path(emqx_storm, RelativePath).

read_schema_configs(App, SchemaFile, ConfigFile) ->
    ct:pal("Read configs - SchemaFile: ~p, ConfigFile: ~p", [SchemaFile, ConfigFile]),
    Schema = cuttlefish_schema:files([SchemaFile]),
    Conf = conf_parse:file(ConfigFile),
    NewConfig = cuttlefish_generator:map(Schema, Conf),
    Vals = proplists:get_value(App, NewConfig, []),
    [application:set_env(App, Par, Value) || {Par, Value} <- Vals].

set_special_configs(emqx_storm) ->
    application:set_env(emqx_storm, host, "127.0.0.1");
set_special_configs(_App) ->
    ok.
