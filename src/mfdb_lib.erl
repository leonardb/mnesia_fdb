%%% @doc RocksDB update  wrappers, in separate module for easy tracing and mocking.
%%%
-module(mfdb_lib).

-compile({no_auto_import, [put/2, get/1]}).

-export([put/3,
         delete/2,
         bin_split/1,
         bin_join_parts/1,
         decode_key/2,
         decode_val/3,
         encode_key/2,
         encode_prefix/2,
         save_parts/4,
         idx_matches/3,
         idx_count_key/2,
         table_count/1,
         table_data_size/1,
         update_counter/4,
         unixtime/0,
         sort/2]).

-include("mfdb.hrl").
-define(SECONDS_TO_EPOCH, (719528*24*3600)).
-define(ENTRIES_PER_COUNTER, 50).

put(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, hca_ref = HcaRef, ttl = TTL}, K, V0) when is_atom(MTab) ->
    %% Operation is on a data table
    ?dbg("Data insert: ~p", [{K, V0}]),
    EncKey = encode_key(TableId, {?DATA_PFX, K}),
    V1 = term_to_binary(V0),
    Size = byte_size(V1),
    %% Add size header as first 32-bits of value
    V = <<Size:32, V1/binary>>,
    %%Tx = erlfdb:create_transaction(Db),
    Fun = fun(Tx) ->
                  case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
                      not_found ->
                          %% New entry, so increase count and add size
                          ok = tbl_count_inc(Tx, TableId, 1),
                          ok = tbl_size_inc(Tx, TableId, Size),
                          ok = put_(Tx, TableId, HcaRef, Size, EncKey, V);
                      OldVal when OldVal =:= V ->
                          %% same key/value
                          ok;
                      <<"mfdb_ref", OldSize:32/integer, OldMfdbRefPartId/binary>> ->
                          %% Replacing entry, increment by size diff
                          {?DATA_PART_PFX, PartHcaVal} = erlfdb_tuple:unpack(OldMfdbRefPartId),
                          Start = encode_prefix(TableId, {?DATA_PART_PFX, PartHcaVal, <<"_">>, '_'}),
                          ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
                          ok = tbl_size_inc(Tx, TableId, ((OldSize * -1) + Size)),
                          ok = put_(Tx, TableId, HcaRef, Size, EncKey, V);
                      <<OldSize:32, _/binary>> ->
                          %% Exists, but value has changed
                          ok = tbl_size_inc(Tx, TableId, ((OldSize * -1) + Size)),
                          ok = put_(Tx, TableId, HcaRef, Size, EncKey, V)
                  end,
                  put_ttl(Tx, TableId, TTL, K)
          end,
    ok = erlfdb:transactional(Db, Fun);
put(#st{db = ?IS_DB = Db, mtab = {_, index, {KeyPos, _}}, index = Indexes}, {V0, K}, {[]}) ->
    ?dbg("Adding data index: ~p ~p ~p", [K, V0, Indexes]),
    %% indexes handled in data put
    case element(KeyPos, Indexes) of
        undefined ->
            ok;
        Idx ->
            IdxFun = fun(Tx) ->
                             ok = add_data_indexes_(Tx, K, V0, Idx)
                     end,
            ok = erlfdb:transactional(Db, IdxFun)
    end,
    ok.

put_(Tx, TableId, HcaRef, Size, EncKey, V) ->
    case Size > ?MAX_VALUE_SIZE of
        true ->
            %% Save the new parts
            MfdbRefPartId = save_parts(Tx, TableId, HcaRef, V),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<?DATA_REF_PFX/binary, Size:32, MfdbRefPartId/binary>>));
        false ->
            erlfdb:wait(erlfdb:set(Tx, EncKey, V))
    end.

put_ttl(_Tx, _TableId, undefined, _Key) ->
    ok;
put_ttl(Tx, TableId, TTL, Key) ->
    %% We need to be able to lookup in both directions
    %% since we use a range query for reaping expired records
    %% and we also need to remove the previous entry if a record gets updated
    ttl_remove_(Tx, TableId, TTL, Key),
    Now = unixtime(),
    ?dbg("Adding TTL Keys: ~p and ~p", [{?TTL_TO_KEY_PFX, Now, Key}, {?KEY_TO_TTL_PFX, Key}]),
    erlfdb:wait(erlfdb:set(Tx, encode_key(TableId, {?TTL_TO_KEY_PFX, Now, Key}), <<>>)),
    erlfdb:wait(erlfdb:set(Tx, encode_key(TableId, {?KEY_TO_TTL_PFX, Key}), integer_to_binary(Now, 10))).

ttl_remove_(_Tx, _TableId, undefined, _Key) ->
    ok;
ttl_remove_(Tx, TableId, _TTL, Key) ->
    ?dbg("Removing TTL refs: ~p ~p", [TableId, Key]),
    TtlK2T = encode_key(TableId, {?KEY_TO_TTL_PFX, Key}),
    case erlfdb:wait(erlfdb:get(Tx, TtlK2T)) of
        not_found ->
            ok;
        Added ->
            OldTtlT2K = {?TTL_TO_KEY_PFX, binary_to_integer(Added, 10), Key},
            erlfdb:wait(erlfdb:clear(Tx, encode_key(TableId, OldTtlT2K)))

    end,
    erlfdb:wait(erlfdb:clear(Tx, encode_key(TableId, {?KEY_TO_TTL_PFX, Key}))).

add_data_indexes_(Tx, K, Val, #idx{table_id = TableId}) ->
    ok = idx_count_inc(Tx, TableId, Val, 1),
    ?dbg("Add data index ~p", [{?DATA_INDEX_PFX, Val, K}]),
    ok = erlfdb:wait(erlfdb:set(Tx, encode_key(TableId, {?DATA_INDEX_PFX, Val, K}), <<>>)).

remove_data_indexes_(Tx, Val, Key, #idx{table_id = TableId}) ->
    ok = idx_count_inc(Tx, TableId, Val, -1),
    ?dbg("Remove data index ~p", [{?DATA_INDEX_PFX, Val, Key}]),
    ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, encode_prefix(TableId, {?DATA_INDEX_PFX, Val, Key}))).

parts_value_(MfdbRefPartId, TableId, Tx) ->
    {?DATA_PART_PFX, PartHcaVal} = erlfdb_tuple:unpack(MfdbRefPartId),
    Start = encode_prefix(TableId, {?DATA_PART_PFX, PartHcaVal, <<"_">>, '_'}),
    Parts = erlfdb:wait(erlfdb:get_range_startswith(Tx, Start)),
    bin_join_parts(Parts).

delete(#st{db = Db, mtab = {_, index, {KeyPos, _}}, index = Indexes}, {V0, Key}) ->
    %% noop - indexes handled in data handlers
    ?dbg("Delete data index: {~p, ~p}", [V0, Key]),
    case element(KeyPos, Indexes) of
        undefined ->
            ok;
        Idx ->
            Tx = erlfdb:create_transaction(Db),
            ok = remove_data_indexes_(Tx, V0, Key, Idx),
            ok = erlfdb:wait(erlfdb:commit(Tx))
    end;
delete(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, ttl = TTL}, K) when is_atom(MTab) ->
    %% deleting a data item
    Tx = erlfdb:create_transaction(Db),
    do_delete_data_(Tx, TableId, true, TTL, K);
delete(#st{db = ?IS_TX = Tx, table_id = TableId, ttl = TTL}, K) ->
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    do_delete_data_(Tx, TableId, false, TTL, K).

do_delete_data_(Tx, TableId, DoCommit, TTL, K) ->
    EncKey = encode_key(TableId, {?DATA_PFX, K}),
    case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            ok;
        <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> ->
            %% decrement size
            ok = tbl_size_inc(Tx, TableId, OldSize * -1),
            %% Remove parts of large value
            {?DATA_PART_PFX, PartHcaVal} = erlfdb_tuple:unpack(MfdbRefPartId),
            Start = encode_prefix(TableId, {?DATA_PART_PFX, PartHcaVal, <<"_">>, '_'}),
            ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
            %% decrement item count
            ok = tbl_count_inc(Tx, TableId, -1);
        <<OldSize:32, _EncVal/binary>> ->
            %% decrement size
            ok = tbl_size_inc(Tx, TableId, OldSize * -1),
            %% decrement item count
            ok = tbl_count_inc(Tx, TableId, -1)
    end,
    ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
    ttl_remove_(Tx, TableId, TTL, K),
    case DoCommit of
        true ->
            ok = erlfdb:wait(erlfdb:commit(Tx));
        false ->
            ok
    end.

idx_matches(#st{db = DbOrTx, index = Indexes}, IdxPos, Key0) ->
    #idx{table_id = TableId} = element(IdxPos, Indexes),
    Pfx = encode_prefix(TableId, {?INDEX_COUNT_PFX, Key0, ?FDB_WC}),
    R = case DbOrTx of
            ?IS_DB ->
                erlfdb:get_range_startswith(DbOrTx, Pfx);
            ?IS_TX ->
                erlfdb:wait(erlfdb:get_range_startswith(DbOrTx, Pfx))
        end,
    case R of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/unsigned-little-integer>>} <- KVs])
    end.

table_data_size(#st{db = DbOrTx, table_id = TableId}) ->
    Pfx = encode_prefix(TableId, {?TABLE_SIZE_PFX, ?FDB_WC}),
    R = case DbOrTx of
            ?IS_DB ->
                erlfdb:get_range_startswith(DbOrTx, Pfx);
            ?IS_TX ->
                erlfdb:wait(erlfdb:get_range_startswith(DbOrTx, Pfx))
        end,
    case R of
        [] ->
            0;
        KVs ->
            Size0 = lists:sum([Count || {_, <<Count:64/unsigned-little-integer>>} <- KVs]),
            erlang:round(Size0 / erlang:system_info(wordsize))
    end.

table_count(#st{db = DbOrTx, table_id = TableId}) ->
    Pfx = encode_prefix(TableId, {?TABLE_COUNT_PFX, ?FDB_WC}),
    R = case DbOrTx of
            ?IS_DB ->
                erlfdb:get_range_startswith(DbOrTx, Pfx);
            ?IS_TX ->
                erlfdb:wait(erlfdb:get_range_startswith(DbOrTx, Pfx))
        end,
    case R of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/unsigned-little-integer>>} <- KVs])
    end.

bin_split(Bin) ->
    bin_split(Bin, 0, []).

bin_split(<<>>, _, Acc) ->
    Acc;
bin_split(<<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>, Inc, Acc) ->
    bin_split(Rest, Inc + 1, [{Inc, Part} | Acc]);
bin_split(Tail, Inc, Acc) ->
    bin_split(<<>>, Inc, [{Inc, Tail} | Acc]).

bin_join_parts(BinList) ->
    bin_join_parts_(lists:keysort(1, BinList), <<>>).

bin_join_parts_([], Acc) ->
    Acc;
bin_join_parts_([{_, Bin} | Rest], Acc) ->
    bin_join_parts_(Rest, <<Acc/binary, Bin/binary>>).

decode_key(TableId, EncKey) ->
    case erlfdb_tuple:unpack(EncKey, TableId) of
        {?DATA_INDEX_PFX, Val, Id} ->
            %% data index reference key
            {idx, {{Val, Id}}};
        {?DATA_PFX, Key} ->
            Key;
        {?INDEX_COUNT_PFX, Key} ->
            {cnt_idx, Key};
        {?DATA_PART_PFX, PartHcaVal} ->
            PartHcaVal;
        {?TTL_TO_KEY_PFX, _, Key} ->
            Key;
        {?KEY_TO_TTL_PFX, Key} ->
            Key;
        BadVal ->
            exit({TableId, BadVal})
    end.

decode_val(_Db, _TableId, <<>>) ->
    <<>>;
decode_val(Db, TableId, <<"mfdb_ref", _OldSize:32, MfdbRefPartId/binary>>) ->
    PartsVal = parts_value_(MfdbRefPartId, TableId, Db),
    <<_:32, Val>> = bin_join_parts(PartsVal),
    binary_to_term(Val);
decode_val(_Db, _TableId, <<_:32, CodedVal/binary>>) ->
    binary_to_term(CodedVal).

encode_key(TableId, Key) when is_tuple(Key) ->
    erlfdb_tuple:pack(Key, TableId).

encode_prefix(TableId, Key0) when is_tuple(Key0) ->
    Key = mk_prefix_(Key0),
    erlfdb_tuple:pack(Key, TableId).

mk_prefix_(Key0) when is_tuple(Key0) ->
    mk_prefix_(tuple_to_list(Key0), []).

mk_prefix_([], Acc) ->
    list_to_tuple(lists:reverse(Acc));
mk_prefix_(['_' | _Rest], Acc) ->
    mk_prefix_([], Acc);
mk_prefix_([Item | Rest], Acc) ->
    mk_prefix_(Rest, [Item | Acc]).

tbl_count_key(TableId) ->
    encode_key(TableId, {?TABLE_COUNT_PFX, rand:uniform(?ENTRIES_PER_COUNTER)}).

tbl_count_inc(_Tx, _TableId, 0) ->
    ok;
tbl_count_inc(Tx, TableId, Inc) when Inc < 1 ->
    %% decrement
    Pfx = encode_prefix(TableId, {?TABLE_COUNT_PFX, ?FDB_WC}),
    case erlfdb:wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            ok;
        Counters0 ->
            lists:foldl(
              fun(_, ok) ->
                      ok;
                 ({K, <<OldVal:64/unsigned-little-integer>>}, waiting) when (OldVal + Inc) >= 0 ->
                      ok = erlfdb:wait(erlfdb:add(Tx, K, Inc));
                 (_, R) ->
                      R
              end, waiting, shuffle(Counters0))
    end;
tbl_count_inc(Tx, TableId, Inc) ->
    %% Increment random counter
    Key = tbl_count_key(TableId),
    erlfdb:wait(erlfdb:add(Tx, Key, Inc)).

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), Item} || Item <- List])].

tbl_size_key(TableId) ->
    encode_key(TableId, {?TABLE_SIZE_PFX, rand:uniform(?ENTRIES_PER_COUNTER)}).

tbl_size_inc(_Tx, _TableId, 0) ->
    ok;
tbl_size_inc(Tx, TableId, Inc) when Inc < 1 ->
    %% decrement
    Pfx = encode_prefix(TableId, {?TABLE_SIZE_PFX, ?FDB_WC}),
    case erlfdb:wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            ok;
        Counters0 ->
            lists:foldl(
              fun(_, ok) ->
                      ok;
                 ({K, <<OldVal:64/unsigned-little-integer>>}, waiting) when (OldVal + Inc) >= 0 ->
                      erlfdb:wait(erlfdb:add(Tx, K, Inc)),
                      ok;
                 (_, R) ->
                      R
              end, waiting, shuffle(Counters0))
    end;
tbl_size_inc(Tx, TableId, Inc) ->
    %% Increment random counter
    Key = tbl_size_key(TableId),
    erlfdb:wait(erlfdb:add(Tx, Key, Inc)).

idx_count_key(TableId, Value) ->
    encode_key(TableId, {?INDEX_COUNT_PFX, Value, rand:uniform(?ENTRIES_PER_COUNTER)}).

idx_count_inc(Tx, TableId, Value, Inc) when Inc < 1 ->
    %% decrement
    Pfx = encode_prefix(TableId, {?INDEX_COUNT_PFX, Value, ?FDB_WC}),
    case erlfdb:wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            ok;
        Counters0 ->
            lists:foldl(
              fun(_, ok) ->
                      ok;
                 ({K, <<OldVal:64/unsigned-little-integer>>}, waiting) when (OldVal + Inc) >= 0 ->
                      erlfdb:wait(erlfdb:add(Tx, K, Inc)),
                      ok;
                 (_, R) ->
                      R
              end, waiting, shuffle(Counters0))
    end;
idx_count_inc(Tx, TableId, Value, Inc) ->
    %% Increment random counter
    Key = idx_count_key(TableId, Value),
    erlfdb:wait(erlfdb:add(Tx, Key, Inc)).

save_parts(?IS_TX = Tx, TableId, Hca, Bin) ->
    PartId = erlfdb_hca:allocate(Hca, Tx),
    PartKey = erlfdb_tuple:pack({?DATA_PART_PFX, PartId}),
    ok = save_parts_(Tx, TableId, PartId, 0, Bin),
    PartKey.

save_parts_(_Tx, _TableId, _PartId, _PartInc, <<>>) ->
    ok;
save_parts_(Tx, TableId, PartId, PartInc, <<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>) ->
    Key = encode_key(TableId, {?DATA_PART_PFX, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Part)),
    save_parts_(Tx, TableId, PartId, PartInc + 1, Rest);
save_parts_(Tx, TableId, PartId, PartInc, Tail) ->
    Key = encode_key(TableId, {?DATA_PART_PFX, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Tail)),
    save_parts_(Tx, TableId, PartId, PartInc + 1, <<>>).

update_counter(Tx, TableId, Key, Incr) ->
    %% Atomic counter increment
    %% Mnesia counters are dirty only, and cannot go below zero
    %%  dirty_update_counter({Tab, Key}, Incr) -> NewVal | exit({aborted, Reason})
    %%    Calls mnesia:dirty_update_counter(Tab, Key, Incr).
    %%
    %%  dirty_update_counter(Tab, Key, Incr) -> NewVal | exit({aborted, Reason})
    %%    Mnesia has no special counter records. However, records of
    %%    the form {Tab, Key, Integer} can be used as (possibly disc-resident)
    %%    counters when Tab is a set. This function updates a counter with a positive
    %%    or negative number. However, counters can never become less than zero.
    %%    There are two significant differences between this function and the action
    %%    of first reading the record, performing the arithmetics, and then writing the record:
    %%
    %%    It is much more efficient. mnesia:dirty_update_counter/3 is performed as an
    %%    atomic operation although it is not protected by a transaction.
    %%    If two processes perform mnesia:dirty_update_counter/3 simultaneously, both
    %%    updates take effect without the risk of losing one of the updates. The new
    %%    value NewVal of the counter is returned.
    %%
    %%    If Key do not exists, a new record is created with value Incr if it is larger
    %%    than 0, otherwise it is set to 0.
    %%
    %% This implementation tries to follow the same pattern.
    %% Increment the counter and then read the new value
    %%  - if the new value is < 0 (not allowed), abort
    %%  - if the new value is > 0, return the new value
    %% The exception:
    %%   if the counter does not exist and is created with a Incr < 0
    %%    we abort instead of creating a counter with value of '0'
    %% NOTE: FDB counters will wrap: EG Incr 0 by -1 wraps to max value
    do_update_counter(Tx, TableId, Key, Incr).

do_update_counter(Tx, TableId, Key, Inc) when Inc < 1 ->
    Pfx = encode_prefix(TableId,  {?TABLE_COUNT_PFX, Key, ?FDB_WC}),
    case erlfdb:wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            {ok, 0};
        Counters0 ->
            %% Decrement a counter where it'll remain >= 0
            %% and sum all the counters at the same time
            {_, NCount} =
                lists:foldl(
                  fun({K, <<OldVal:64/unsigned-little-integer>>}, waiting) when (OldVal + Inc) >= 0 ->
                          erlfdb:wait(erlfdb:add(Tx, K, Inc)),
                          {ok, OldVal + Inc};
                     ({_, <<OldVal:64/unsigned-little-integer>>}, {R, IInc}) ->
                          {R, OldVal + IInc}
                  end, {waiting, 0}, shuffle(Counters0)),
            {ok, NCount}
    end;
do_update_counter(Tx, TableId, Key, Inc) ->
    %% Increment random counter
    Key = encode_key(TableId,  {?TABLE_COUNT_PFX, Key, rand:uniform(?ENTRIES_PER_COUNTER)}),
    ok = erlfdb:wait(erlfdb:add(Tx, Key, Inc)),
    %% Then sum all the counters
    Pfx = encode_prefix(TableId,  {?TABLE_COUNT_PFX, Key, ?FDB_WC}),
    case erlfdb:wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            {ok, 0};
        KVs ->
            lists:sum([Val || {_, <<Val:64/unsigned-little-integer>>} <- KVs])
    end.

unixtime() ->
    datetime_to_unix(erlang:universaltime()).

datetime_to_unix({Mega, Secs, _}) ->
    (Mega * 1000000) + Secs;
datetime_to_unix({{Y,Mo,D},{H,Mi,S}}) ->
    calendar:datetime_to_gregorian_seconds(
      {{Y,Mo,D},{H,Mi,round(S)}}) - ?SECONDS_TO_EPOCH.

sort(_RecName, [] = L) ->
    L;
sort(RecName, [T | _] = L) when is_tuple(T) andalso element(1, T) =:= RecName ->
    lists:keysort(2, L);
sort(_RecName, L) ->
    lists:sort(L).
