build:
	GOTOOLCHAIN=go1.24rc1 GOOS=linux GOARCH=arm64 go build -o bin/fluffh main.go

deps:
	GOTOOLCHAIN=go1.24rc1 go mod download


example:
	GOTOOLCHAIN=go1.24rc1 go run indexgen/indexgen.go example
