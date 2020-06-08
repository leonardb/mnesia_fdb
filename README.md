# mnesia_fdb

A FoundationDB backend for Mnesia.

Status: very 'alpha' **warning. It may eat your data**

This permits Erlang/OTP applications to use FoundationDB as a backend for
mnesia tables. It is based on Aeternity's `mnesia_rocksdb` which was based on Klarna's `mnesia_eleveldb`.

Contributions *very* welcome

## FoundationDB

https://apple.github.io/foundationdb/

FoundationDB is a distributed database designed to handle large volumes of structured 
data across clusters of commodity servers. It organizes data as an ordered key-value 
store and employs ACID transactions for all operations. It is especially well-suited 
for read/write workloads but also has excellent performance for write-intensive 
workloads.

Since FoundationDB is ACID everything is transactional, and mnesia:dirty_ functions are
still transactional

## Prerequisites
- foundationdb client (and server as necessary)
- erlfdb (included as dependency)
- Erlang/OTP 20.0 or newer (https://github.com/erlang/otp)

## Getting started

mnesia_fdb requires cluster connection settings be configured in application env

- `cluster` (required) :: binary() :: Path the to fdb.cluster file
- `tls_cert_path` :: binary() :: Path the to .pem used for FDB connection
- `tls_key_path` :: binary() :: Path the to .key used for FDB connection
- `tls_ca_path` :: binary() :: Path the to .crt used for FDB connection

Example:
```
[
 {mnesia_fdb,
  [
   {cluster, <<"/etc/foundationdb/fdb.local.cluster">>},
   {tls_cert_path, <<"/etc/foundationdb/fdb.local.pem">>},
   {tls_key_path, <<"/etc/foundationdb/fdb.local.key">>},
   {tls_ca_path, <<"/etc/foundationdb/fdb.local.crt">>}
  ]
 }
].
```

Call `mnesia_fdb:register()` immediately after
starting mnesia.

Put `{fdb_copies, [node()]}` into the table definitions of
tables you want to be in FoundationDB.


## Special features

FoundationDB tables is an ordered key/value store.

To support the concept of tables an HCA is used to assign prefix values
for tables as they are created. This prefix is then used for all keys
within the table.

Each record also uses a 'DataPrefix'
- <<"d">> for set/ordered_set tables
- <<"b">> for bag tables
- <<"p">> for 'parts' of values larger than 90Kb

### Key internals (set/ordered_set)
 - keys are encoded using `erlfdb_tuple_pack('{<<"d">>, sext:encode(Key)}, Prefix)`
 
### Key internals (bag)
  - bag tables use an HCA pre table for assigning suffixes to duplicate keys
  - keys are encoded using `erlfdb_tuple_pack('{<<"b">>, sext:encode(Key), Incr}, Prefix)`

### Large values
 FoundationDb limits the size of values to 100Kb. `mnesia_fdb` splits larger values into 90Kb chunks.
 They are stored using a table specific Parts HCA for assigning references.
 The actual value store with the Key is a binary in the form <<"mfdb_ref", MfdbRefPrefix/binary>>.
 The MfdbRefPrefix is then used to gather and join the parts when the data is needed.

## Create a table

```erlang
rd(test, {id, value}).
mnesia:create_table(foo, [{fdb_copies, [node()]},
                          {attributes, record_info(fields, test)},
                          {record_name,test},
                          {type, set}]).
```

## Handling of errors in write operations

The FoundationDB update operations return either `ok` or `{error, any()}`.
Since the actual updates are performed after the 'point-of-no-return',
returning an `error` result will cause mnesia to behave unpredictably,
since the operations are expected to simply work.

## Caveats

Avoid placing `bag` tables in `mnesia_fdb`. Although they work, each write
requires additional reads, causing substantial runtime overheads. There
are better ways to represent and process bag data.

The `mnesia:table_info(T, size)` call always returns zero for FoundationDB
tables. FoundationDB itself does not track the number of elements in a table.

## TODO and Ideas
- [ ] Proper support for continuations (currently we process the entire match set)
- [ ] Possibly use the FDB directory layer instead of relying on HCA prefixes?
- [ ] Secondary index support
