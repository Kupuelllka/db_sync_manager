.PHONY: build run test clean

build:
	go build -o bin/db_swapper ./cmd/db_swapper

run: build
	./bin/db_swapper

test:
	go test -v ./...

clean:
	rm -rf bin/