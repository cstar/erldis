ERL=erl
# store output so is only executed once
ERL_LIBS=$(shell erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell)
# get application vsn from app file
VSN=$(shell erl -pa ebin/ -eval 'application:load(erldis), {ok, Vsn} = application:get_key(erldis, vsn), io:format("~s~n", [Vsn])' -s init stop -noshell)

all: src

src: FORCE
	@$(ERL) -make

clean:
	rm -f erl_crash.dump *.beam */*.beam

TEST_SOURCES := $(wildcard test/*.erl)
TEST_MODULES = $(TEST_SOURCES:test/%.erl=%)

test: src FORCE $(TEST_MODULES)

./$(TEST_MODULES):
	@echo "Running tests for $@"
	erlc -pa ebin/ -o ebin/ -I include/ test/$@.erl
	erl -pa ebin/ -run $@ test -run init stop -noshell

install: all	
	mkdir -p $(ERL_LIBS)/erldis-$(VSN)/ebin
	mkdir -p $(ERL_LIBS)/erldis-$(VSN)/include
	for i in ebin/*.beam; do install $$i $(ERL_LIBS)/erldis-$(VSN)/$$i ; done
	for i in include/*.hrl; do install $$i $(ERL_LIBS)/erldis-$(VSN)/$$i ; done
	# also install .app file
	install ebin/erldis.app $(ERL_LIBS)/erldis-$(VSN)/ebin/erldis.app
	install ebin/erldis.appup $(ERL_LIBS)/erldis-$(VSN)/ebin/erldis.appup

plt:
	@dialyzer --build_plt --output_plt .plt -q -r . -I include/

check: all
	@dialyzer --check_plt --plt .plt -q -r . -I include/

FORCE: