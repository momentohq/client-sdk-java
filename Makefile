.PHONY: clean
## Clean the project
clean:
	./gradlew clean

.PHONY: build
## Build the project
build:
	./gradlew build

.PHONY: test
## Run all the tests
test: test-unit test-integration

.PHONY: test-unit
## Run the unit tests
test-unit:
	./gradlew test

.PHONY: test-integration
## Run the integration tests
test-integration:
	./gradlew intTest

.PHONY: format
## Format the code
format:
	./gradlew spotlessApply

.PHONY: lint
## Lint the code
lint:
	./gradlew spotlessCheck

.PHONY: precommit
## Run the precommit checks
precommit: format lint test

.PHONY: help
# See <https://gist.github.com/klmr/575726c7e05d8780505a> for explanation.
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)";echo;sed -ne"/^## /{h;s/.*//;:d" -e"H;n;s/^## //;td" -e"s/:.*//;G;s/\\n## /---/;s/\\n/ /g;p;}" ${MAKEFILE_LIST}|LC_ALL='C' sort -f|awk -F --- -v n=$$(tput cols) -v i=19 -v a="$$(tput setaf 6)" -v z="$$(tput sgr0)" '{printf"%s%*s%s ",a,-i,$$1,z;m=split($$2,w," ");l=n-i;for(j=1;j<=m;j++){l-=length(w[j])+1;if(l<= 0){l=n-i-length(w[j])-1;printf"\n%*s ",-i," ";}printf"%s ",w[j];}printf"\n";}'|more $(shell test $(shell uname) == Darwin && echo '-Xr')
