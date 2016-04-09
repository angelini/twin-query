#!/bin/bash

set -ex

rm -f data/sample.db

cargo run add data/sample.db data/foo.schema data/foo.csv
cargo run add data/sample.db data/bar.schema data/bar.csv

rm -f data/music.db

python ./gen_data.py

cargo run add data/music.db data/artists.schema data/artists.csv
cargo run add data/music.db data/albums.schema data/albums.csv
cargo run add data/music.db data/tracks.schema data/tracks.csv
