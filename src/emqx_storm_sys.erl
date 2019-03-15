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

-export([nodes/1, stats/1, metrics/1, connections/1, sessions/1, topics/1, subscriptions/1]).

nodes(Binding = #{node := _Node}) ->
    emqx_mgmt_api_nodes:get(Binding, params).

stats(Bindings = #{node := Node}) when Node =:= node() ->
    emqx_mgmt_api_stats:lookup(Bindings, params);
stats(_) ->
    emqx_mgmt_api_stats:list(#{}, params).

metrics(Bindings = #{node := Node}) when Node =:= node() ->
    emqx_mgmt_api_metrics:list(Bindings, params);
metrics(_) ->
    emqx_mgmt_api_metrics:list(#{}, params).

connections(Bindings = #{node := Node, page := PageNum, limit := Limit})
  when Node =:= node() ->
    emqx_mgmt_api_connections:list(Bindings, params(PageNum, Limit));
connections(_) ->
    emqx_mgmt_api_connections:list(#{node => node()}, params(1, 20)).

sessions(Bindings = #{node := Node, page := PageNum, limit := Limit})
  when Node =:= node() ->
    emqx_mgmt_api_sessions:list(Bindings, params(PageNum, Limit));
sessions(_) ->
    emqx_mgmt_api_sessions:list(#{node => node()}, params(1, 20)).
    
topics(#{page := PageNum, limit := Limit}) ->
    emqx_mgmt_api_routes:list(#{}, params(PageNum, Limit));
topics(_) ->
    emqx_mgmt_api_routes:list(#{node => node()}, params(1, 20)).

subscriptions(Bindings = #{node := Node, page := PageNum, limit := Limit}) 
  when Node =:= node() ->
    emqx_mgmt_api_subscriptions:list(Bindings, params(PageNum, Limit));
subscriptions(_Bindings) ->
    emqx_mgmt_api_subscriptions:list(#{}, params(1, 20)).

params(PageNum, Limit) ->
    [{<<"_page">>, PageNum}, {<<"_limit">>, Limit}].
