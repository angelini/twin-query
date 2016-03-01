#!/bin/bash

set -ex

rm -f data/sample.db
rm -f data/gdp.db

cargo run add data/sample.db data/foo.schema data/foo.csv
cargo run add data/sample.db data/bar.schema data/bar.csv
cargo run add data/gdp.db data/gdp.schema data/gdp.csv
