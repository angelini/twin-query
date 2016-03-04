# twin-query

Data is stored as columns of triplets

```
                      Db
                     /  \
       --------------    --------------
       |                              |
  table.column                   table.column
       |                              |
(id, value, time)              (id, value, time)
(id, value, time)              (id, value, time)
(id, value, time)              (id, value, time)
(id, value, time)              (id, value, time)
(id, value, time)              (id, value, time)
```

Queries have the following form

```
s <table.column>[, <table.column>]      # select
j <table> on <table.column>             # join
w <table.column> <operator> <constant>  # where
l <size>                                # limit
```

Add data to a new Db from multiple CSV files

```
$ twin-query add sample.db data/foo.schema data/foo.csv
$ twin-query add sample.db data/bar.schema data/bar.csv
```

Start a query REPL using the new Db

```
$ twin-query repl sample.db

>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

s bar.a, bar.b, bar.c, bar.foo, foo.a, foo.b, foo.c

<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

+-------------+----------------+---------------------+------------+-----------+---------------+------------------+
| bar.a       | bar.b          | bar.c               | bar.foo    | foo.a     | foo.b         | foo.c            |
+-------------+----------------+---------------------+------------+-----------+---------------+------------------+
| (4, 11, 11) | (4, true, 11)  | (4, "first 2", 11)  | (4, 0, 11) | (0, 1, 1) | (0, true, 1)  | (0, "first", 1)  |
| (5, 22, 22) | (5, true, 22)  | (5, "second 2", 22) | (5, 0, 22) | (1, 2, 2) | (1, true, 2)  | (1, "second", 2) |
| (6, 33, 33) | (6, false, 33) | (6, "third 2", 33)  | (6, 1, 33) | (2, 3, 3) | (2, false, 3) | (2, "third", 3)  |
| (7, 44, 44) | (7, false, 44) | (7, "fourth 2", 44) | (7, 2, 44) | (3, 4, 4) | (3, false, 4) | (3, "fourth", 4) |
+-------------+----------------+---------------------+------------+-----------+---------------+------------------+

>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

s bar.b
j foo on bar.foo
w foo.a < 2
  foo.c = "first"

<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

+---------------+
| bar.b         |
+---------------+
| (4, true, 11) |
| (5, true, 22) |
+---------------+
```
