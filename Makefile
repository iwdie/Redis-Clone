.PHONY: build run test fmt vet clean

build:
	go build -o bin/redis-clone .

run:
	go run . -addr :6379

test:
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -rf bin *.aof
