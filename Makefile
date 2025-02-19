NTHREADS=$(shell nproc --all 2>/dev/null || sysctl -n hw.logicalcpu)
CFLAGS+=-O2 -march=native -mtune=native -flto -Wall -Wextra -Wpedantic \
-Wformat=2 -Wconversion -Wundef -Winline -Wimplicit-fallthrough -DNTHREADS=$(NTHREADS)

-O2 -march=native -mtune=native -flto -Wall -Wextra -Wpedantic -Wformat=2 -Wconversion -Wundef -Winline -Wimplicit-fallthrough

ifdef DEBUG
CFLAGS+= -D_FORTIFY_SOURCE=2 -D_GLIBCXX_ASSERTIONS 	\
-fsanitize=address -fsanitize=undefined -g 									\
-fstack-protector-strong
endif

all: bin/ bin/create-sample bin/analyze

gadominguez: bin/gadominguez

bin/:
	mkdir -p bin/

.PHONY: profile
profile: bin/analyze
	perf record --call-graph dwarf bin/analyze measurements-1M.txt

bin/create-sample: create-sample.c
	$(CC) $(CFLAGS) $^ -lm -o $@

bin/analyze: analyze.c
	$(CC) $(CFLAGS) $^ -o $@

bin/gadominguez: gadominguez.cpp
	$(CXX) $(CFLAGS) $^ -o $@	

.PHONY: clean
clean:
	rm -r bin/
