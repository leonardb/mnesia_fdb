# mnesia_fdb

A FoundationDB backend for Mnesia.

Permits Erlang/OTP applications to use FoundationDB as a backend for
mnesia tables. It is based on Aeternity's `mnesia_rocksdb` which was based on Klarna's `mnesia_eleveldb`.

Contributions and feedback are welcome.

### TODO / Help Wanted
- [ ] Secondary Indexes
- [ ] Watch table definition keys for changes (eg, index added on a different node)
- [ ] Investigate possibility of using the FDB directory layer rather than using packed prefixes
- [ ] Performance improvements, especially in the iterator
- [ ] Backup/restore


## FoundationDB

https://apple.github.io/foundationdb/

FoundationDB is a distributed database designed to handle large volumes of structured 
data across clusters of commodity servers. It organizes data as an ordered key-value 
store and employs ACID transactions for all operations. It is especially well-suited 
for read/write workloads but also has excellent performance for write-intensive 
workloads.

Since FoundationDB is ACID, everything is transactional, and mnesia:dirty_ functions are
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

FoundationDB is an ordered key/value store.

To support the concept of tables in the mnesia backend an HCA is used 
to assign prefix values for tables as they are created. This prefix 
is then used for all keys within the table.

Each record also uses a 'DataPrefix'
- <<"d">> for set/ordered_set tables
- <<"b">> for bag tables
- <<"p">> for 'parts' of values larger than 90Kb

### Key internals (set/ordered_set)
 - set/ordered_set keys are encoded using `sext:encode({TableId, <<"d">>, Key})`
 
### Key internals (bag)
  - bag tables use an HCA pre table for assigning suffixes to duplicate keys
  - keys are encoded using `sext:encode({TableId, <<"b">>, Key, BagHac})`

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
                          {type, ordered_set}]).
```

## Handling of errors in write operations

The FoundationDB update operations return either `ok` or `{error, any()}`.
Since the actual updates are performed after the 'point-of-no-return',
returning an `error` result will cause mnesia to behave unpredictably,
since the operations are expected to simply work.

## Caveats

While `bag` tables are supported there is substantial runtime overhead as they
require additional reads. There may be better ways to represent and process bag data.

The `mnesia:table_info(T, size)` call always returns zero for FoundationDB
tables. FoundationDB itself does not track the number of elements in a table.

FoundationDB is limited with both key and value sizes. Keys < 10Kb and Values < 100Kb.
In mnesia_fdb we have a hard limit of 9Kb for keys, and values are automatically split.

Since secondary indexes in mnesia are stored as {{Value,Id}} keys, this makes
supporting indexing of large values impossible.

In order to support secondary indexes there is a hard limit on the sizes of values
for indexed fields. This limit includes internal components.

If a value for an indexed column exceeds the size limit an exception may occur or an informational error may be returned.

Examples: 
```erlang
rd(test,{id,value,expires}).

mnesia:create_table(test1, [{fdb_copies,[node()]},{type,ordered_set},{record_name, test},{attributes,record_info(fields,test)}]).

mnesia:add_table_index(test1,#test.expires).

Big = <<1:(1024*1024)>>.

mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = 100}).
ok

W = fun(F) -> try F() of R -> io:format("Got result: ~p~n", [R]) catch E:M -> io:format("E: ~p~nM: ~p~n",[E,M]) end end.

76> W(fun() -> mnesia:transaction(fun() -> mnesia:write(test1, #test{id = 6, value = Big, expires = Big}, write) end) end).
Got result: {aborted,{value_too_large_for_field,expires}}
ok
77> W(fun() -> mnesia:activity(transaction, fun() -> mnesia:write(test1, #test{id = 6, value = Big, expires = Big}, write) end) end).
E: exit
M: {aborted,{value_too_large_for_field,expires}}
ok
78> W(fun() -> mnesia:activity(sync, fun() -> mnesia:write(test1, #test{id = 6, value = Big, expires = Big}, write) end) end).       
Got result: {aborted,{bad_type,sync}}
ok
79> W(fun() -> mnesia:activity(async, fun() -> mnesia:write(test1, #test{id = 6, value = Big, expires = Big}, write) end) end).
Got result: {aborted,{bad_type,async}}
ok
80> W(fun() -> mnesia:activity(dirty_sync, fun() -> mnesia:write(test1, #test{id = 6, value = Big, expires = Big}, write) end) end). 
Got result: {aborted,{bad_type,dirty_sync}}
ok
81> W(fun() -> mnesia:activity(dirty_async, fun() -> mnesia:write(test1, #test{id = 6, value = Big, expires = Big}, write) end) end).
Got result: {aborted,{bad_type,dirty_async}}
ok
82> W(fun() -> mnesia:transaction(fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end) end).           
Got result: {aborted,{value_too_large_for_field,expires}}
ok
83> W(fun() -> mnesia:activity(transaction, fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end) end).
E: exit
M: {aborted,{value_too_large_for_field,expires}}
ok
84> W(fun() -> mnesia:activity(sync, fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end) end).       
Got result: {aborted,{bad_type,sync}}
ok
85> W(fun() -> mnesia:activity(async, fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end) end).
Got result: {aborted,{bad_type,async}}
ok
86> W(fun() -> mnesia:activity(dirty_sync, fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end) end). 
Got result: {aborted,{bad_type,dirty_sync}}
ok
87> W(fun() -> mnesia:activity(dirty_async, fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end) end).
Got result: {aborted,{bad_type,dirty_async}}
ok
88> W(fun() -> mnesia:dirty_write(test1, #test{id = 6, value = Big, expires = Big}) end).           
E: error
M: {badmatch,{error,{value_too_large_for_field,expires}}}
ok


```