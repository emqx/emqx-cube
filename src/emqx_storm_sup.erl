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

-module(emqx_storm_sup).

-include("emqx_storm.hrl").
-include_lib("stdlib/include/qlc.hrl").

-behaviour(supervisor).

-define(SERVER, ?MODULE).

%% API
-export([start_link/0,
         storms/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts storm supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
                      {error, {already_started, Pid :: pid()}} |
                      {error, {shutdown, term()}} |
                      {error, term()} |
                      ignore.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart intensity, and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  {ok, {SupFlags :: supervisor:sup_flags(),
                        [ChildSpec :: supervisor:child_spec()]}} |
                  ignore.
init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 100,
                 period => 10},
    ok = ekka_mnesia:create_table(configuration, [{disc_copies, [node()]},
                                                  {attributes, record_info(fields, configuration)}]),
    ok = ekka_mnesia:copy_table(configuration, disc_copies),
    Config = application:get_env(?APP, storms, []),
    ok = lists:foreach(fun add_default_config/1, Config),
    StormOpts = storm_options(),
    Storm = [storm_spec(Opts) || Opts <- StormOpts],
    {ok, {SupFlags, Storm}}.

-spec(storms() -> any()).
storms() ->
    [{Name, emqx_storm:status(Pid)} || {Name, Pid, _, _} <- supervisor:which_children(?MODULE)].

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
