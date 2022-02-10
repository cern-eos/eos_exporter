.PHONY: build

descr:
	@echo "You are building the EOS exporter binary."

build:
	go generate
	go build .

run:
	go run eos_exporter.go

clean:
	@rm -f .build_date .git_commit .go_version .version eos_exporter

all: descr build
