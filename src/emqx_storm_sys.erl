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

-module(emqx_storm_sys).

-export([ nodes/1
        , stats/1
        , metrics/1
        , connections/1
        , sessions/1
        , topics/1
        , subscriptions/1]).

nodes(Bindings) ->
    emqx_mgmt_api_nodes:get(Bindings, params).

stats(Bindings = #{node := _Node}) ->
    emqx_mgmt_api_stats:lookup(Bindings, params);
stats(Bindings) ->
    emqx_mgmt_api_stats:lookup(Bindings#{node => node()}, params).

metrics(Bindings) ->
    emqx_mgmt_api_metrics:list(Bindings, params).

connections(Bindings = #{'_page':= PageNum, '_limit' := Limit})->
    emqx_mgmt_api_connections:list(Bindings#{ node => node() }, params(PageNum, Limit)).

sessions(Bindings = #{'_page' := PageNum, '_limit' := Limit}) ->
    emqx_mgmt_api_sessions:list(Bindings, params(PageNum, Limit)).
    
topics(#{'_page' := PageNum, '_limit' := Limit}) ->
    emqx_mgmt_api_routes:list(#{}, params(PageNum, Limit)).

subscriptions(Bindings = #{'_page' := PageNum, '_limit' := Limit}) ->
    emqx_mgmt_api_subscriptions:list(Bindings, params(PageNum, Limit)).

params(PageNum, Limit) ->
    [{<<"_page">>, integer_to_binary(PageNum)},
     {<<"_limit">>, integer_to_binary(Limit)}].
