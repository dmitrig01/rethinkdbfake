rethinkdbfake
=============

## Motivation

ReQL is a great language. Perhaps one of the best parts is that it is implemented in pure JavaScript. Unfortunately, testing large codebases using Rethink can be slow, as the server is slow to create and delete new tables. To remedy that, the other half of RethinkDB has been implemented in pure Javascript – a mechanism that can take ReQL queries and evaluate them against an in-memory datastore. Obviously this is ideal for very small data sets – exactly what one would find in a testing environment.

## Roadmap
* Most types are converted to their native JS equivalent. This has a few unfortunate implications, namely (1) the handling of what should return a cursor and what should return a straight array is very approximate and (2) since there is no way to distinguish between an object and a SingleSelection or a Stream and StreamSelection, updates can be run where they shouldn't be. To move forward, every function executed should return a [Rethink type](https://github.com/rethinkdb/rethinkdb/blob/next/src/rdb_protocol/ql2.proto), which is only converted to a native JS type at the very end.
* The order of function execution is backwards – this starts at the beginning of the query and works towards the end, instead of the other way around. This prevents .default() from working properly (and likely has other implications as well).
* Certain functions are not implemented:
    * Between
    * Many date functions
    * Default doesn't properly work
* Backtraces are not implemented (though they should be relatively trivial).
