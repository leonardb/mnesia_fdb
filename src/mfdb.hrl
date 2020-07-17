
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

%% FDB has limits on key and value sizes
-define(MAX_VALUE_SIZE, 92160). %% 90Kb in bytes
-define(MAX_KEY_SIZE, 9216). %% 9Kb in bytes

-define(TABLE_PREFIX, <<"tbl_">>).
-define(FDB_WC, '_').
-define(FDB_END, <<"~">>).
-define(DATA_PREFIX, <<"d">>).
-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).
-define(IS_FUTURE, {erlfdb_future, _, _}).
-define(IS_FOLD_FUTURE, {fold_info, _, _}).
-define(IS_SS, {erlfdb_snapshot, _}).
-define(IS_ITERATOR, {cont, #iter_st{}}).
-define(GET_TX(SS), element(2, SS)).
-define(SORT(L), lists:keysort(2, L)).

-type db() :: {erlfdb_database, reference()}.
-type tx() :: {erlfdb_transaction, reference()}.
-type selector() :: {binary, gteq | gt | lteq | lt}.
-type idx() :: {atom(), index, {pos_integer(), atom()}}.

-record(conn,
        {
         id      = conn :: conn,
         cluster        :: binary(),                %% Absolute path, incl filename, of fdb.cluster file
         tls_ca_path    :: undefined | binary(),    %% Absolute path, incl filename, of CA certificate
         tls_key_path   :: undefined | binary(),    %% Absolute path, incl filename, of Private Key
         tls_cert_path  :: undefined | binary()     %% Absolute path, incl filename, of SSL Certificate
        }).

-record(idx,
        {
         mtab                       :: idx(), %% keep in 1st position as unique key
         tab                        :: binary(),
         table_id                   :: binary(),
         pos                        :: integer(),
         index_consistent = false   :: boolean()
        }).

-record(st,
        {
         tab                            :: binary(),
         mtab                           :: any(), %% mnesia table spec
         alias                          :: atom(),
         record_name                    :: atom(),
         attributes                     :: list(atom()),
         index                          :: tuple(),
         on_write_error         = ?WRITE_ERR_DEFAULT :: on_write_error(),
         on_write_error_store   = ?WRITE_ERR_STORE_DEFAULT :: on_write_error_store(),
         db                             :: db(),
         table_id                       :: binary(),
         hca_ref,   %% opaque :: #erlfdb_hca{} record used for mfdb_part() keys    :: erlfdb_hca:create(<<"parts_", TableId/binary>>).
         info                   = []
        }).

-record(info, {k, v}).

-record(iter_st,
        {
         db :: db(),
         tx :: tx(),
         table_id :: binary(),
         tab :: binary(),
         mtab :: any(), %% mnesia table id
         data_count = 0 :: non_neg_integer(),
         data_limit = 0 :: non_neg_integer(),
         data_acc = [],
         data_fun :: undefined | function(),
         acc_keys = false :: boolean(),
         keys_only = false :: boolean(),
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

%% enable debugging messages through mnesia:set_debug_level(debug)
-ifndef(MNESIA_FDB_NO_DBG).
-define(dbg(Fmt, Args),
        %% avoid evaluating Args if the message will be dropped anyway
        case mnesia_monitor:get_env(debug) of
            none -> ok;
            verbose -> ok;
            _ -> mnesia_lib:dbg_out("~p:~p: "++(Fmt)++"~n",[?MODULE,?LINE|Args])
        end).
-else.
-define(dbg(Fmt, Args), ok).
-endif.
