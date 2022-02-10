#!/bin/bash
git rev-parse --short HEAD > .git_commit
date +%FT%T%z > .build_date
git describe --always > .version
go version | awk '{print $3}' > .go_version
