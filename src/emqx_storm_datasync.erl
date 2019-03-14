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

-include("emqx_storm.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-export([list/1,
         update/1,
         lookup/1,
         add/1,
         start/1,
         stop/1,
         status/1]).

list(_Bindings) ->
    {ok, emqx_storm_cfg:all_bridges()}.

update(#{id := Id, options := Options}) ->
    emqx_storm_cfg:update_bridge(Id, Options).

lookup(#{id := Id}) ->
    emqx_storm_cfg:lookup_bridge(Id).

add(#{id := Id, options := Options}) ->
    emqx_storm_cfg:add_bridge(Id, Options).

start(#{id := Id}) ->
    emqx_storm_cfg:start_bridge(Id).

stop(#{id := Id}) ->
    emqx_storm_cfg:stop_bridge(Id).

status(_Bindings) ->
    emqx_storm_cfg:bridge_status().
