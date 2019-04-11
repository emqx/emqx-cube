%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-record(bridges, {id       :: atom() | list(),
                  name     :: atom() | list(),
                  options  :: tuple()}).

-type bridges() :: #bridges{}.

-define(APP, emqx_storm).

-define(TAB, bridges).

%% Return Codes
-define(SUCCESS, 200). %% Success
-define(ERROR1, 400).  %% Data resolved fail
-define(ERROR2, 404).  %% Actions and types do not exist
-define(ERROR3, 422).  %% Lack tid and clientid
-define(ERROR4, 502).  %% Resource unreachable
-define(ERROR5, 500).  %% Unknown error
