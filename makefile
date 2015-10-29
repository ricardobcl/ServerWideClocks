REBAR3_URL=https://s3.amazonaws.com/rebar3/rebar3

# If there is a rebar in the current directory, use it
ifeq ($(wildcard rebar3),rebar3)
REBAR3 = $(CURDIR)/rebar3
endif

# Fallback to rebar on PATH
REBAR3 ?= $(shell test -e `which rebar3` 2>/dev/null && which rebar3 || echo "./rebar3")

# And finally, prep to download rebar if all else fails
ifeq ($(REBAR3),)
REBAR3 = $(CURDIR)/rebar3
endif

all: $(REBAR3)
	@$(REBAR3) do clean, deps, compile, eunit, ct, dialyzer

rel: all
	@$(REBAR3) release

clean:
	@$(REBAR3) clean

compile:
	@$(REBAR3) compile

deps:
	@$(REBAR3) deps

doc: compile
	@$(REBAR3) edoc

test: deps compile
	@$(REBAR3) do eunit, ct, dialyzer


$(REBAR3):
	curl -Lo rebar3 $(REBAR3_URL) || wget $(REBAR3_URL)
	chmod a+x rebar3
