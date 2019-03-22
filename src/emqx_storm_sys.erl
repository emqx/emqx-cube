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

nodes(Bindings = #{node := _Node}) ->
    emqx_mgmt_api_nodes:get(Bindings, params);
nodes(Bindings) ->
    emqx_mgmt_api_nodes:get(Bindings#{node => node()}, params).

stats(Bindings = #{node := _Node}) ->
    emqx_mgmt_api_stats:lookup(Bindings, params);
stats(Bindings) ->
    emqx_mgmt_api_stats:lookup(Bindings#{node => node()}, params).

metrics(_Bindings) ->
    emqx_mgmt:return({ok, emqx_metrics:all()}).

connections(Bindings = #{'_page':= PageNum, '_limit' := Limit})->
    emqx_mgmt_api_connections:list(Bindings#{ node => node() }, params(PageNum, Limit));
connections(Bindings) ->
    emqx_mgmt_api_connections:list(Bindings#{ node => node() }, params(1, 20)).

sessions(Bindings = #{'_page' := PageNum, '_limit' := Limit}) ->
    emqx_mgmt_api_sessions:list(Bindings#{ node => node() }, params(PageNum, Limit));
sessions(Bindings) ->
    emqx_mgmt_api_sessions:list(Bindings#{ node => node() }, params(1, 20)).


topics(#{'_page' := PageNum, '_limit' := Limit}) ->
    emqx_mgmt_api_routes:list(#{}, params(PageNum, Limit));
topics(_Bindings) ->
    emqx_mgmt_api_routes:list(#{}, params(1, 20)).

subscriptions(Bindings = #{'_page' := PageNum, '_limit' := Limit}) ->
    emqx_mgmt_api_subscriptions:list(Bindings#{ node => node() }, params(PageNum, Limit));
subscriptions(Bindings) ->
    emqx_mgmt_api_subscriptions:list(Bindings#{ node => node() }, params(1, 20)).

params(PageNum, Limit) ->
    [{<<"_page">>, maybe_itb(PageNum)},
     {<<"_limit">>, maybe_itb(Limit)}].

maybe_itb(I) when is_integer(I) ->
    integer_to_binary(I);
maybe_itb(I) ->
    I.
