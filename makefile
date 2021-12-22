# Builds project
build:
	go build ./...

# Runs tests on whole project prints the result
# on the standard output
test:
	go test ./...
	
# As above but does that without cashing the results
test-no-cache:
	go test -count=1 ./...
