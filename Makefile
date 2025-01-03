build:
	GOOS=linux GOARCH=arm64 go build -o bin/fluffh main.go

deps:
	go mod download

exampledir:
	go run indexgen/indexgen.go example
