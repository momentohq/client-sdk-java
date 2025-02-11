.PHONY: all clean build test prod-test test-unit test-integration test-auth-service test-cache-service \
    test-leaderboard-service test-storage-service test-topics-service test-http-service format lint precommit help

all: precommit

## Clean the project
clean:
	./gradlew clean

## Build the project
build:
	./gradlew build

## Run all the tests
test: test-unit test-integration

## Run the unit tests
test-unit:
	./gradlew test

## Run all integration tests
test-integration:
	./gradlew integrationTest

## Run all integration tests with consistent reads enabled
prod-test:
	@CONSISTENT_READS=1 $(MAKE) test-integration

## Run the auth service tests
test-auth-service:
	@CONSISTENT_READS=1 ./gradlew test-auth-service

## Run the cache service tests
test-cache-service:
	@CONSISTENT_READS=1 ./gradlew test-cache-service

## Run the leaderboard service tests
test-leaderboard-service:
	@CONSISTENT_READS=1 ./gradlew test-leaderboard-service

## Run the storage service tests
test-storage-service:
	@CONSISTENT_READS=1 ./gradlew test-storage-service

## Run the topics service tests
test-topics-service:
	@CONSISTENT_READS=1 ./gradlew test-topics-service

## Run the http service tests
test-http-service:
	@echo "No tests for http service."

## Run the retry service tests
test-retry-service:
	./gradlew test-retry-service

## Format the code
format:
	./gradlew spotlessApply

## Lint the code
lint:
	./gradlew spotlessCheck

## Run the precommit checks
precommit: format lint build test

# See <https://gist.github.com/klmr/575726c7e05d8780505a> for explanation.
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)";echo;sed -ne"/^## /{h;s/.*//;:d" -e"H;n;s/^## //;td" -e"s/:.*//;G;s/\\n## /---/;s/\\n/ /g;p;}" ${MAKEFILE_LIST}|LC_ALL='C' sort -f|awk -F --- -v n=$$(tput cols) -v i=19 -v a="$$(tput setaf 6)" -v z="$$(tput sgr0)" '{printf"%s%*s%s ",a,-i,$$1,z;m=split($$2,w," ");l=n-i;for(j=1;j<=m;j++){l-=length(w[j])+1;if(l<= 0){l=n-i-length(w[j])-1;printf"\n%*s ",-i," ";}printf"%s ",w[j];}printf"\n";}'|more $(shell test $(shell uname) == Darwin && echo '-Xr')
