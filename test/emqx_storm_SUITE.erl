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

-define(BRIDGE1, {bridge1, [{address, "127.0.0.1"}]}).
-define(BRIDGE2, {bridge2, [{address, "127.0.0.2"}]}).
-define(BRIDGE3, {bridge3, [{address, "127.0.0.3"}]}).

-define(CONTROL, <<"storm/control/qg3rgewt135">>).
-define(ACK, <<"storm/ack/qg3rgewt135">>).

-define(BRIDGES, [?BRIDGE1, ?BRIDGE2, ?BRIDGE3]).

all() ->
    [{group, emqx_storm}].

groups() ->
    [{emqx_storm, [sequence],
      [ storm_t
      , datasync_t
      , sys_t]}].

init_per_suite(Config) ->
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, c),
    %% dbg:tpl(emqx_storm, start_link, x),
    %% dbg:tpl(emqx_storm, init, x),
    %% dbg:tpl(emqx_client, subscribe, x),
    %% dbg:tpl(emqx_storm, handle_msg, x),
    %% dbg:tpl(emqx_storm, handle_payload, x),
    %% dbg:tpl(emqx_storm, handle_request, x),
    %% dbg:tpl(emqx_storm_sys, stats, x),
    %% dbg:tpl(emqx_storm, encode_result, x),
    dbg:tpl(emqx_storm, make_rsp_msg, x),
    %% dbg:tpl(emqx_storm, return, x),
    %% dbg:tpl(emqx_storm, restruct, x),
    dbg:tpl(emqx_storm, send_response, x),
    dbg:tpl(emqx_client, publish, x),
    %% dbg:tpl(emqx_client, eval_msg_handler, x),
    dbg:tpl(emqx_client, publish_process, x),
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
    ct:print("start test sys"),
    {ok, C} = emqx_client:start_link(),
    {ok, _} = emqx_client:connect(C),
    {ok, _, [1]} = emqx_client:subscribe(C, ?ACK, qos1),
    {ok, _} = emqx_client:publish(C, ?CONTROL, construct(sys, <<"stats">>, <<>>), 1),
    ?assert(receive_response()),
    {ok, _} = emqx_client:publish(C, ?CONTROL, construct(sys, <<"stats">>, <<>>), 1),
    ?assert(receive_response()),
    ok = emqx_client:disconnect(C).

test_datasync() ->
    ok.

construct(sys, Action, _Payload) ->
    Tuple = [{tid, <<"111">>},
             {type, <<"sys">>},
             {action, Action}],
    emqx_json:encode(Tuple).

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
    lists:foreach(fun({Id, Options}) ->
                      emqx_storm_datasync:add_bridge(Id, Options)
                  end, ?BRIDGES),
    %% ct:log("~p", [ets:tab2list(bridges)]),
    ?assertEqual(3, length(Parse(emqx_storm_datasync:all_bridges()))),
    emqx_storm_datasync:update_bridge(bridge1, [{address, "127.0.0.4"}]),
    ?assertEqual({bridge1, [{address, "127.0.0.4"}]},
                 Parse(emqx_storm_datasync:lookup(#{id => bridge1}))),
    emqx_storm_datasync:delete(#{id => bridge3}),
    ?assertEqual(2, length(Parse(emqx_storm_datasync:list(#{})))),
    emqx_storm_datasync:add_bridge(test, bridge_spec()),
    emqx_storm_datasync:start(#{id => test}),
    ?assertEqual([{test, connected}], Parse(emqx_storm_datasync:status(#{}))),
    emqx_storm_datasync:stop(#{id => test}),
    ?assertEqual([], Parse(emqx_storm_datasync:status(#{}))),
    ok.

bridge_spec() ->
    [{address,"127.0.0.1:1883"},
     {clean_start,true},
     {client_id,"bridge_aws"},
     {connect_module,emqx_bridge_mqtt},
     {forwards,["topic1/#","topic2/#"]},
     {keepalive,60000},
     {max_inflight_batches,32},
     {mountpoint,"bridge/aws/${node}/"},
     {password,"passwd"},
     {proto_ver,v4},
     {queue,
      #{batch_bytes_limit => 1048576000,batch_count_limit => 32,
        replayq_dir => "data/emqx_aws_bridge/",
        replayq_seg_bytes => 10485760}},
     {reconnect_delay_ms,30000},
     {retry_interval,20000},
     {ssl,false},
     {ssl_opts,
      [{versions,['tlsv1.2','tlsv1.1',tlsv1]},
       [{keyfile,"etc/certs/client-key.pem"},
        [{certfile,"etc/certs/client-cert.pem"},
         [{cacertfile,"etc/certs/cacert.pem"},[]]]]]},
     {start_type,manual},
     {subscriptions,[{"cmd/topic1",1},{"cmd/topic2",1}]},
     {username,"user"}].

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
