# Builds project
.PHONY: build
build:
	go build ./...

# Runs tests on whole project prints the result
# on the standard output
.PHONY: test
test:
	go test ./...
	
# As above but does that without cashing the results
.PHONY: test-no-cache
test-no-cache:
	go test -count=1 ./...
