#!/bin/bash

set -ex

rm -f data/sample.db

cargo run add data/sample.db data/foo.schema data/foo.csv
cargo run add data/sample.db data/bar.schema data/bar.csv
