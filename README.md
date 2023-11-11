### RUN-LENGTH-EXAMPLE

Basic HTTP server that exposes a collection of sequences.

Provides:

- Batch inserts of key / value pairs at current or specific time interval
- Queries with automatic grouping interval selection (max number of points)
- Basic data persistence (file)
- Basic retention policy
- Basic UI to demo a few common queries

This example heavily relies on the host time.

### Usage
```
Usage of ./server:
  -f string
    	Full path to dump file (default "./store.dump")
  -i int
    	Dump interval in seconds (0 or less to disable)
  -l string
    	Listening address:port (default "127.0.0.1:8080")
  -r int
    	Retention policy in days (0 or less to disable) (default 365)
```

### Endpoints

#### POST `/insert/`

Batch insert multiple key / value pairs at current or specific time interval.

Body format:
```
key1 value1 [unixTime1]
key2 value2 [unixTime2]
key3 value3 [unixTime3]
```

Examples:
```
curl -X POST --data $'k1 1\nk2 0\nk3 1' http://127.0.0.1:8080/insert/
curl -X POST --data $'k1 1 1692316800' http://127.0.0.1:8080/insert/
```

#### GET `/query/`

Perform a query for a key / time range.

A grouping interval will be automatically selected according to the value of `maxNumberOfPoints`.

Example:
```
curl 'http://127.0.0.1:8080/query/?key=k1&start=1692316800&end=1692403199'
```