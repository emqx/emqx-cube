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

-module(emqx_cube_sup).

-include("emqx_cube.hrl").
-include_lib("stdlib/include/qlc.hrl").

-behaviour(supervisor).

-define(SUP, ?MODULE).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts cube supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
                      {error, {already_started, Pid :: pid()}} |
                      {error, {shutdown, term()}} |
                      {error, term()} |
                      ignore.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, ?SUP).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% init cube supervisor
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  {ok, {SupFlags :: supervisor:sup_flags(),
                        [ChildSpec :: supervisor:child_spec()]}} |
                  ignore.
init(?SUP) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 100,
                 period => 10},
    Options = application:get_all_env(?APP),
    {ok, {SupFlags, [cube_spec(Options)]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec cube_spec(Args :: tuple()) ->
                        ChildSpec :: supervisor:child_spec().
cube_spec(Options) ->
    #{id       => cube,
      start    => {emqx_cube, start_link, [Options]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [emqx_cube]}.
