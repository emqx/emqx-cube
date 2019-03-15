PROJECT = emqx_storm
PROJECT_DESCRIPTION = EMQ X Storm
PROJECT_VERSION = 3.0

NO_AUTOPATCH = cuttlefish

CUR_BRANCH := $(shell git branch | grep -e "^*" | cut -d' ' -f 2)
BRANCH := $(if $(filter $(CUR_BRANCH), master develop), $(CUR_BRANCH), develop)

DEPS = jsx
dep_jsx    = git-emqx https://github.com/talentdeficit/jsx 2.9.0

BUILD_DEPS = emqx cuttlefish emqx_management
dep_emqx       = git-emqx https://github.com/emqx/emqx $(BRANCH)
dep_cuttlefish = git-emqx https://github.com/emqx/cuttlefish v2.2.1
dep_emqx_management = git-emqx https://github.com/emqx/emqx-management $(BRANCH)

ERLC_OPTS += +debug_info

TEST_ERLC_OPTS += +debug_info

EUNIT_OPTS = verbose

COVER = true

CT_SUITES = emqx_storm

CT_OPTS = -erl_args -name emqx_storm_ct@127.0.0.1

$(shell [ -f erlang.mk ] || curl -s -o erlang.mk https://raw.githubusercontent.com/emqx/erlmk/master/erlang.mk)
include erlang.mk

CUTTLEFISH_SCRIPT = _build/default/lib/cuttlefish/cuttlefish

app.config: $(CUTTLEFISH_SCRIPT) etc/emqx_storm.conf
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/emqx_storm.conf -i priv/emqx_storm.schema -d data

$(CUTTLEFISH_SCRIPT): rebar-deps
	@if [ ! -f cuttlefish ]; then make -C _build/default/lib/cuttlefish; fi

distclean::
	@rm -rf _build cover deps logs log data
	@rm -f rebar.lock compile_commands.json cuttlefish

rebar-deps:
	rebar3 get-deps

rebar-clean:
	@rebar3 clean

rebar-compile: rebar-deps
	rebar3 compile

rebar-ct: app.config
	rebar3 ct -v

rebar-xref:
	@rebar3 xref
