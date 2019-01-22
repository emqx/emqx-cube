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
                intensity => 10,
                period => 100},
    StormOpts = application:get_env(?APP, storms, []),
    Storm = [child_spec(Opts) || Opts <- StormOpts],
    {ok, {Supflag, Storm}}.

child_spec({Id, Options}) ->
    #{id       => Id,
      start    => {emqx_storm, start_link, [Id, Options]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [emqx_storm]}.
