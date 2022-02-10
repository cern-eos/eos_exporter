#!/bin/bash
git rev-parse --short HEAD | tr -d "\n" > .git_commit
date +%FT%T%z | tr -d "\n" > .build_date
git describe --always | tr -d "\n" > .version
go version | awk '{print $3}' | tr -d "\n" > .go_version
