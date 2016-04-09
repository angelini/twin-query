#!/usr/bin/env python

from argparse import ArgumentParser
from faker import Factory
from random import randint

faker = Factory.create()


def clean(s):
    return str(s).replace('.', '').replace(',', '')


def gen_data(suffix, artist_count, album_count, track_count):
    artists = [(i, faker.year(), faker.name()) for i in range(artist_count)]

    albums = []
    for i in range(album_count):
        artist = artists[randint(0, artist_count - 1)]
        albums.append((i, faker.year(), faker.name(), artist[0], randint(1, 10), faker.company()))

    tracks = []
    for i in range(track_count):
        album = albums[randint(0, album_count - 1)]
        artist_id = album[3]
        album_year = album[1]
        tracks.append((i, album_year, faker.last_name(), artist_id, album[0], randint(100, 300)))

    return artists, albums, tracks


def write(suffix, artists, albums, tracks):
    with open('data/artists{}.csv'.format(suffix), 'w') as f:
        for artist in artists:
            f.write(','.join(map(clean, artist)) + '\n')

    with open('data/artists{}.schema'.format(suffix), 'w') as f:
        f.write("""
table = "artists"
csv_ordering = ["id", "time", "name"]

[columns]
name = "String"
""")

    with open('data/albums{}.csv'.format(suffix), 'w') as f:
        for album in albums:
            f.write(','.join(map(clean, album)) + '\n')

    with open('data/albums{}.schema'.format(suffix), 'w') as f:
        f.write("""
table = "albums"
csv_ordering = ["id", "time", "name", "artist_id", "tracks", "label"]

[columns]
name = "String"
artist_id = "Int"
tracks = "Int"
label = "String"
""")

    with open('data/tracks{}.csv'.format(suffix), 'w') as f:
        for track in tracks:
            f.write(','.join(map(clean, track)) + '\n')

    with open('data/tracks{}.schema'.format(suffix), 'w') as f:
        f.write("""
table = "tracks"
csv_ordering = ["id", "time", "name", "artist_id", "album_id", "length"]

[columns]
name = "String"
artist_id = "Int"
album_id = "Int"
length = "Int"
""")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--suffix', default='')
    parser.add_argument('--artists', type=int, default=10)
    parser.add_argument('--albums', type=int, default=40)
    parser.add_argument('--tracks', type=int, default=100)
    args = parser.parse_args()

    artists, albums, tracks = gen_data(args.suffix, args.artists, args.albums, args.tracks)
    write(args.suffix, artists, albums, tracks)
