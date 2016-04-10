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

# rm -f data/music_big.db

# python ./gen_data.py --suffix="_big" --artists=1000000 --albums=4000000 --tracks=10000000

# cargo run --release add data/music_big.db data/artists_big.schema data/artists_big.csv
# cargo run --release add data/music_big.db data/albums_big.schema data/albums_big.csv
# cargo run --release add data/music_big.db data/tracks_big.schema data/tracks_big.csv
