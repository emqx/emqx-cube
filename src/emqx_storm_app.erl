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
-module(emqx_storm_app).

-include("emqx_storm.hrl").
-include_lib("stdlib/include/qlc.hrl").

-behaviour(application).
-behaviour(supervisor).

-export([start/2, stop/1]).

-define(APP, emqx_storm).

-define(SERVER, ?MODULE).

%% Supervisor callbacks
-export([init/1]).

-export([storms/0]).

%%--------------------------------------------------------------------
%% @doc
%% Starts the application and supervisor
%% @end
%%--------------------------------------------------------------------
-spec start(StartType :: normal |
                         {takeover, Node :: node()} |
                         {failover, Node :: node()},
            StartArgs :: term()) -> {ok, Pid :: pid()} |
                                    {error, {already_started, Pid :: pid()}} |
                                    {error, {shutdown, term()}} |
                                    {error, term()} |
                                    ignore.
start(_Type, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(State :: term()) -> any().
stop(_State) ->
    ok.

-spec(storms() -> any()).
storms() ->
    [{Name, emqx_storm:status(Pid)} || {Name, Pid, _, _} <- supervisor:which_children(?MODULE)].

%%--------------------------------------------------------------------
%% Start supervisor
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  {ok, {SupFlags :: supervisor:sup_flags(),
                        [ChildSpec :: supervisor:child_spec()]}} |
                  ignore.
init([]) ->
    Supflag = #{strategy => one_for_one,
                intensity => 100,
                period => 10},
    ok = ekka_mnesia:create_table(configuration, [{disc_copies, [node()]},
                                                  {attributes, record_info(fields, configuration)}]),
    ok = ekka_mnesia:copy_table(configuration, disc_copies),
    DefaultOpts = application:get_env(?APP, storms, []),
    ok = lists:foreach(fun add_default_config/1, DefaultOpts),
    StormOpts = storm_options(),
    Storm = [storm_spec(Opts) || Opts <- StormOpts],
    {ok, {Supflag, Storm}}.

-spec storm_spec(Args :: tuple()) ->
                        ChildSpec :: supervisor:child_spec().
storm_spec({Name, Options}) ->
    #{id       => Name,
      start    => {emqx_storm, start_link, [Name, Options]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [emqx_storm]}.

-spec storm_options() -> ok.
storm_options() ->
    QueryOptions = fun() ->
                       Q = qlc:q([{Config#configuration.id, Config#configuration.options } 
                                  || Config <- mnesia:table(configuration)]),
                       qlc:e(Q)
                   end,
    {atomic, Configs} = mnesia:transaction(QueryOptions),
    Configs.

-spec add_default_config(DefaultConfig :: tuple()) -> ok | {error, any()}.
add_default_config({Id, Options}) ->
    add_config(Id, Options).

-spec add_config(Id :: atom() | list(), Options :: tuple()) -> ok | {error, any()}.
add_config(Id, Options) ->
    Config = #configuration{id = Id, options = Options},
    return(mnesia:transaction(fun insert_config/1, [Config])).

-spec insert_config(Config :: configuration()) ->  ok | no_return().
insert_config(Config = #configuration{id = Id}) ->
    case mnesia:read(configuration, Id) of
        [] -> mnesia:write(Config);
        [_ | _] -> mnesia:abort(existed)
    end.

-spec return(Args :: {atomic, ok} | {aborted, any()})
            -> ok | {error, Error :: any()}.
return({atomic, ok})     -> ok;
return({aborted, Error}) -> {error, Error}.
