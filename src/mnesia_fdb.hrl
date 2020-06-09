
-record(sel, { alias                            % TODO: not used
             , tab
             , table_id
             , db
             , keypat
             , lastval
             , ms                               % TODO: not used
             , compiled_ms
             , limit
             , key_only = false                 % TODO: not used
             , direction = forward              % TODO: not used
             }).

-type on_write_error() :: debug | verbose | warning | error | fatal.
-type on_write_error_store() :: atom() | undefined.

-define(WRITE_ERR_DEFAULT, verbose).
-define(WRITE_ERR_STORE_DEFAULT, undefined).

-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(GB, 1024 * 1024 * 1024).
-define(TABLE_PREFIX, <<"tbl_">>).
-define(FDB_WC, '_').
-define(FDB_END, <<"~">>).
-define(MAX_VALUE_SIZE, 92160). %% 90Kb in Bytes
-define(DATA_PREFIX(T), case T of bag -> <<"b">>; _ -> <<"d">> end).
-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).
-define(IS_FUTURE, {erlfdb_future, _, _}).
-define(IS_FOLD_FUTURE, {fold_info, _, _}).
-define(IS_SS, {erlfdb_snapshot, _}).
-define(IS_ITERATOR, {cont, #iter_st{}}).
-define(GET_TX(SS), element(2, SS)).

-type db() :: {erlfdb_database, reference()}.
-type tx() :: {erlfdb_transaction, reference()}.
-type selector() :: {binary, gteq | gt | lteq | lt}.

-record(conn,
        {
         id      = conn :: conn,
         cluster        :: binary(),                %% Absolute path, incl filename, of fdb.cluster file
         tls_ca_path    :: undefined | binary(),    %% Absolute path, incl filename, of CA certificate
         tls_key_path   :: undefined | binary(),    %% Absolute path, incl filename, of Private Key
         tls_cert_path  :: undefined | binary()     %% Absolute path, incl filename, of SSL Certificate
        }).

-record(st,
        {
         tab                   :: atom(),
         type                   = set   :: set | ordered_set | bag,
         alias                          :: atom(),
         record_name                    :: atom(),
         attributes                     :: list(atom()),
         index                  = []    :: list(pos_integer()),
         on_write_error         = ?WRITE_ERR_DEFAULT :: on_write_error(),
         on_write_error_store   = ?WRITE_ERR_STORE_DEFAULT :: on_write_error_store(),
         db                             :: db(),
         tab_bin                        :: binary(),
         table_id                       :: binary(),
         hca_bag,  %% opaque :: #erlfdb_hca{} record used for bag type table keys :: erlfdb_hca:create(<<"hca_", TableId/binary>>).
         hca_ref   %% opaque :: #erlfdb_hca{} record used for mfdb_part() keys    :: erlfdb_hca:create(<<"parts_", TableId/binary>>).
        }).

-record(iter_st,
        {
         db :: db(),
         tx :: tx(),
         table_id :: binary(),
         tab :: atom(),
         type :: atom(),
         data_count = 0 :: non_neg_integer(),
         data_limit = 0 :: non_neg_integer(),
         data_acc = [],
         data_fun :: undefined | function(),
         return_keys_only = false :: boolean(),
         need_keys_only = false :: boolean(),
         compiled_ms,
         start_key :: any(),
         start_sel :: selector(),
         end_sel :: selector(),
         limit :: pos_integer(),
         target_bytes :: integer(),
         streaming_mode :: atom(),
         iteration :: pos_integer(),
         snapshot :: boolean(),
         reverse :: 1 | 0
        }).
