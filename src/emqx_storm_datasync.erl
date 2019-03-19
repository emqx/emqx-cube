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
-include_lib("stdlib/include/qlc.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-else.
-export([mnesia/1]).
-export([ list/1
        , update/1
        , lookup/1
        , add/1
        , delete/1
        , start/1
        , create/1
        , stop/1
        , status/1
        ]).
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
    ret(update_bridge(Id, trans_opts(Options))).

lookup(#{id := Id}) ->
    {ok, case lookup_bridge(Id) of
             ?NO_BRIDGE ->
                 [{code, ?ERROR4}];
             Bridge ->
                 [{code, ?SUCCESS},
                  {data, Bridge}]
         end}.

add(#{id := Id, options := Options}) ->
    ret(add_bridge(Id, trans_opts(Options))).

delete(#{id := Id}) ->
    ret(remove_bridge(Id)).

start(#{id := Id}) ->
    start_bridge(Id).

create(#{id := Id, options := Options}) ->
    case add_bridge(Id, trans_opts(Options)) of
        {atomic, ok} ->
            start_bridge(Id);
        {aborted, Error} ->
            {ok, [{code, ?ERROR4}, {data, Error}]}
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
            {ok, [{code, ?ERROR4}]}
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
                     _ -> [{code, ?ERROR4},
                           {data, <<"Start bridge failed">>}]
                 catch
                     _Error:_Reason ->
                         [{code, ?ERROR4},
                          {data, <<"Start bridge failed">>}]
                 end;
             _ ->
                 [{code, ?ERROR4},
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
                 [{code, ?ERROR4},
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
ret({aborted, Error}) -> {ok, [{code, ?ERROR4}, {data, Error}]}.

trans_opts(RawArgs) when is_list(RawArgs) ->
    trans_opts(RawArgs, []).

trans_opts([], Acc) ->
    lists:reverse(Acc);
trans_opts([{K0, V0} | RestProps], Acc) ->
    ListB2L = fun(Values) ->
                  lists:map(fun(E) ->
                                binary_to_list(E)
                            end, Values)
              end,
    case {binary_to_atom(K0, utf8), V0} of
        {K1, V1} when K1 =:= connect_module;
                      K1 =:= proto_ver;
                      K1 =:= start_type ->
            trans_opts(RestProps, [{K1, binary_to_atom(V1, utf8)} | Acc]);
        {K2, V2} when K2 =:= address;
                      K2 =:= client_id;
                      K2 =:= mountpoint;
                      K2 =:= password;
                      K2 =:= username ->
            trans_opts(RestProps, [{K2, binary_to_atom(V2, utf8)} | Acc]);
        {K3, V3} when K3 =:= forwards ->
            trans_opts(RestProps, 
                       [{K3, ListB2L(V3)} | Acc]);
        {K4, V4} when K4 =:= ssl_opts ->
            trans_opts(RestProps,
                       [{K4, lists:map(fun({versions, Vers}) ->
                                               lists:map(fun(E) ->
                                                             binary_to_atom(E, utf8)
                                                         end, Vers);
                                          ({_, Values}) ->
                                               ListB2L(Values)
                                       end, V4)} | Acc]);
        {K5, V5} when K5 =:= subscriptions ->
            trans_opts(RestProps,
                       [{K5, lists:map(fun({Topic, QoS}) ->
                                           {binary_to_list(Topic), QoS}
                                       end, V5)} | Acc])
    end.
