## shallow clone for speed

REBAR_GIT_CLONE_OPTIONS += --depth 1
export REBAR_GIT_CLONE_OPTIONS

REBAR = rebar3
all: compile

compile: unlock
	$(REBAR) compile

ct: compile
	$(REBAR) as test ct -v

eunit: compile
	$(REBAR) as test eunit

unlock: unlock
	@rm -rf _build/*/lib/*/rebar.lock
	$(REBAR) unlock

xref:
	$(REBAR) xref

clean: distclean

distclean:
	@rm -rf _build
	@rm -f data/app.*.config data/vm.*.args rebar.lock
