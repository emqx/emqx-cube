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

-define(NO_BRIDGE, undefined).

-include("emqx_storm.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").
-include_lib("stdlib/include/qlc.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-else.
-export([mnesia/1]).
-export([list/1,
         update/1,
         lookup/1,
         add/1,
         delete/1,
         start/1,
         create/1,
         stop/1,
         status/1]).
-endif.

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).


%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {disc_copies, [node()]},
                {record_name, ?TAB},
                {attributes, record_info(fields, ?TAB)},
                {storage_properties, [{ets, [{read_concurrency, true},
                                             {write_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

list(_Bindings) ->
    all_bridges().

update(#{id := Id, options := Options}) ->
    ret(update_bridge(Id, Options)).

lookup(#{id := Id}) ->
    {ok, case lookup_bridge(Id) of
             ?NO_BRIDGE ->
                 [{code, ?ERROR2}];
             Bridge ->
                 [{code, ?SUCCESS},
                  {data, Bridge}]
         end}.

add(#{id := Id, options := Options}) ->
    ret(add_bridge(Id, Options)).

delete(#{id := Id}) ->
    ret(remove_bridge(Id)).

start(#{id := Id}) ->
    start_bridge(Id).

create(#{id := Id, options := Options}) ->
    case add_bridge(Id, Options) of
        {atomic, ok} ->
            start_bridge(Id);
        {aborted, Error} ->
            {ok, [{code, ?ERROR2}, {data, Error}]}
    end.

stop(#{id := Id}) ->
    stop_bridge(Id).

status(_Bindings) ->
    bridge_status().

-spec(all_bridges() -> list()).
all_bridges() -> 
    try ets:tab2list(?TAB) of
        Result -> 
            {ok, [{code, ?SUCCESS},
                  {data, Result}]}
    catch
        _Error:_Reason ->
            {ok, [{code, ?ERROR2}]}
    end.

-spec add_bridge(Id :: atom() | list(), Options :: tuple()) 
                -> {ok, list()}.
add_bridge(Id, Options) ->
    Config = #?TAB{id = Id, options = Options},
    mnesia:transaction(fun insert_bridge/1, [Config]).

-spec insert_bridge(Bridge :: ?TAB()) ->  ok | no_return().
insert_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [] -> mnesia:write(Bridge);
        [_ | _] -> mnesia:abort(existed)
    end.

-spec(update_bridge(atom(), list()) -> {ok, list()}).
update_bridge(Id, Options) ->
    Bridge = #?TAB{id = Id, options = Options},
    mnesia:transaction(fun do_update_bridge/1, [Bridge]).

do_update_bridge(Bridge = #?TAB{id = Id}) ->
    case mnesia:read(?TAB, Id) of
        [_|_] -> mnesia:write(Bridge);
        [] -> mnesia:abort(noexisted)
    end.

-spec(remove_bridge(atom()) -> ok | {error, any()}).
remove_bridge(Id) ->
    mnesia:transaction(fun mnesia:delete/1,
                       [{?TAB, Id}]).

-spec(start_bridge(atom()) -> {ok, list()}).
start_bridge(Id) ->
    {ok, case lookup_bridge(Id) of
             {Id, Option} ->
                 emqx_bridge_sup:create_bridge(Id, Option),
                 try emqx_bridge:ensure_started(Id) of
                     ok -> [{code, ?SUCCESS},
                            {data, <<"Start bridge successfully">>}];
                     connected -> [{code, ?SUCCESS},
                                   {data, <<"Bridge already started">>}];
                     _ -> [{code, ?ERROR2},
                           {data, <<"Start bridge failed">>}]
                 catch
                     _Error:_Reason ->
                         [{code, ?ERROR2},
                          {data, <<"Start bridge failed">>}]
                 end;
             _ ->
                 [{code, ?ERROR2},
                  {data, <<"bridge_not_found">>}]
         end}.

-spec(bridge_status() -> list()).
bridge_status() ->
    {ok, [{code, ?SUCCESS},
          {data, emqx_bridge_sup:bridges()}]}.

-spec(stop_bridge(atom()) -> ok| {error, any()}).
stop_bridge(Id) ->
    {ok, case emqx_bridge_sup:drop_bridge(Id) of
             ok -> 
                 [{code, ?SUCCESS},
                  {data, <<"stop bridge successfully">>}];
             _Error -> 
                 [{code, ?ERROR2},
                  {data, <<"stop bridge failed">>}]
         end}.

%% @doc Lookup bridge by id
-spec(lookup_bridge(atom()) -> tuple() | ?NO_BRIDGE).
lookup_bridge(Id) ->
    case mnesia:dirty_read(?TAB, Id) of
        [{?TAB, Id, Option}] ->
            {Id, Option};
        _ ->
            ?NO_BRIDGE
    end.

-spec ret(Args :: {atomic, ok} | {aborted, any()})
            -> {ok, list()}.
ret({atomic, ok})     -> {ok, [{code, ?SUCCESS}]};
ret({aborted, Error}) -> {ok, [{code, ?ERROR2}, {data, Error}]}.
