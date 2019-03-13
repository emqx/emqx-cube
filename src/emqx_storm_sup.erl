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
    StormOpts = application:get_env(?APP, storms, []),
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
