.PHONY: build clean deploy

build:
	${GOPATH}/bin/dep ensure
	env GOOS=linux go build -ldflags="-s -w" -o bin/hello hello/main.go
	env GOOS=linux go build -ldflags="-s -w" -o bin/world world/main.go

init:
	${GOPATH}/bin/dep init -v

clean:
	rm -rf ./bin

deploy: clean build
	sls deploy --verbose
