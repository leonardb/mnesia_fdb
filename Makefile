suite=$(if $(SUITE), suite=$(SUITE), )
REBAR3=$(shell which rebar3 || echo ./rebar3)

.PHONY: all check test clean run

all:
	$(REBAR3) compile

docs:
	$(REBAR3) doc

check:
	$(REBAR3) dialyzer

eunit:
	ERL_AFLAGS="-config /opt/mnesia_fdb/test.config" $(REBAR3) eunit as test $(suite)

ct:
	$(REBAR3) ct as test $(suite)

test: eunit ct

conf_clean:
	@:

clean:
	$(REBAR3) clean
	$(RM) doc/*

run:
	$(REBAR3) shell
