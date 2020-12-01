# mnesia_fdb

ARCHIVED - Superceded by https://github.com/leonardb/mfdb

Due to issues with the way mnesia manages tables etc it made more sense to build a mnesia-like feature set over FDB than to use it as a backend.


A FoundationDB backend for Mnesia.

**We're looking for contributors.**

Permits Erlang/OTP applications to use FoundationDB as a backend for
mnesia tables. It is based on Aeternity's `mnesia_rocksdb` which was based on Klarna's `mnesia_eleveldb`.

Contributions and feedback are welcome.

**WARNING** This is not considered safe for production yet.

### TODO / Help Wanted
- [ ] Watch table definition keys for changes (eg, index added on a different node)
- [ ] Performance improvements, especially in the iterator
- [ ] Backup/restore
- [ ] Primary/Secondary node concept. Controlling table creation/deletion from a single node


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
- <<"d">> for ordered_set tables
- <<"di">> for data indexes
- <<"p">> for 'parts' of values larger than 90Kb

### Key internals (ordered_set)
 - ordered_set keys are encoded using `sext:encode({TableId, <<"d">>, Key})`
 
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

## Secondary indexes and selects

Mnesia only uses secondary indexes when there is an explicit value match in the matchspec header.
If a comparison is used in the matchspec guard, the secondary index is not used.

Example of default Mnesia behavior:
```
given a table named `test` using record `#test{id :: integer{}, value :: any(), expires :: integer()}`
with secondary indexes on `value` and `expires`.

mnesia:dirty_select(test, [{#test{value = <<"test">>, _ = '_'}, [], ['$_']}]).
This would use the secondary index on `value`

mnesia:dirty_select(test, [{#test{value = '$1', _ = '_'}, [{'=:=','$1', <<"test">>}], ['$_']}]).
This would *not* use the secondary index on `value`
``` 

In order to take advantage of FoundationDBs ordered keys mnesia_fdb will
attempt to rewrite matchspecs, moving head matches into the guards, and then
check if there is a usable secondary index.

The current implementation is fairly naive in the planning of which index to use
and could use further improvement.

In order to facilitate better choices mnesia_fdb maintains internal counters of
matched records for values on secondary indexes and will prefer an index with fewer
matches.

Additionally, mnesia_fdb will attempt to convert range type comparison operators into
FoundationDb range boundaries to prevent full table scans.

Example (mnesia_fdb behavior):
```
mnesia:dirty_select(test, [{#test{expires = '$1', _ = '_'}, [{'>=','$1', 500}, {'<','$1', 1000}], ['$_']}]).
internally mnesia_fdb would convert this into a
range query using the secondary index on `expires`
```

## Caveats

mnesia:table_info(Table, memory) returns an approximate size of the stored data in bytes.
This does not include the size of indexes.

The `mnesia:table_info(T, size)` call returns the count of records in a table.
This should be correct for both ordered_set and bag tables.

`bag` tables are *not* supported.

FoundationDB is limited with both key and value sizes. Keys < 10Kb and Values < 100Kb.
In mnesia_fdb we have a hard limit of 9Kb for keys, while values are automatically split.

**IMPORTANT:** Since secondary indexes use the value as a component of the key, there are
hard limits on the sizes of values for individual fields when a secondary index is defined.

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
