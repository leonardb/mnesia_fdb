
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
-define(MAX_VALUE_SIZE, 92160). %% 90Kb in Bytes
-define(DATA_PREFIX(T), case T of bag -> <<"b">>; _ -> <<"d">> end).
-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).

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
         db                             :: {erlfdb_database, reference()},
         tab_bin                        :: binary(),
         table_id                       :: binary(),
         hca_bag,  %% opaque :: #erlfdb_hca{} record used for bag type table keys :: erlfdb_hca:create(<<"hca_", TableId/binary>>).
         hca_ref   %% opaque :: #erlfdb_hca{} record used for mfdb_part() keys    :: erlfdb_hca:create(<<"parts_", TableId/binary>>).
        }).
