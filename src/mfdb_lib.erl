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
         update_counter/3]).

-include("mfdb.hrl").

put(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, hca_bag = HcaBag, hca_ref = HcaRef, index = Indexes, type = bag}, K, V) ->
    Tx = erlfdb:create_transaction(Db),
    put_bag_(Tx, TableId, MTab, HcaBag, HcaRef, Indexes, {K, V});
put(#st{db = ?IS_TX = Tx, table_id = TableId, mtab = MTab, hca_bag = HcaBag, hca_ref = HcaRef, index = Indexes, type = bag}, K, V) ->
    put_bag_(Tx, TableId, MTab, HcaBag, HcaRef, Indexes, {K, V});
put(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, hca_ref = HcaRef}, K, V0) when is_atom(MTab) ->
    %% Operation is on a data table
    ?dbg("Data insert: ~p", [{K, V0}]),
    EncKey = encode_key(TableId, {<<"d">>, K}),
    V1 = term_to_binary(V0),
    Size = byte_size(V1),
    %% Add size header as first 32-bits of value
    V = <<Size:32, V1/binary>>,
    Tx = erlfdb:create_transaction(Db),
    SizeInc = case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
                  <<"mfdb_ref", OldSize:32, OldMfdbRefPartId/binary>> ->
                      %% Replacing entry, increment by size diff
                      {<<"p">>, PartHcaVal} = sext:decode(OldMfdbRefPartId),
                      Start = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
                      ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
                      ((OldSize * -1) + Size);
                  <<OldSize:32, _/binary>> ->
                      %% Replacing entry, increment by size diff
                      ((OldSize * -1) + Size);
                  not_found ->
                      %% New entry, so increase count and add size
                      ok = erlfdb:wait(erlfdb:add(Tx, tbl_count_key(TableId), 1)),
                      Size
              end,
    ok = erlfdb:wait(erlfdb:add(Tx, tbl_size_key(TableId), SizeInc)),
    case Size > ?MAX_VALUE_SIZE of
        true ->
            %% Save the new parts
            MfdbRefPartId = save_parts(Tx, TableId, HcaRef, V),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", Size:32, MfdbRefPartId/binary>>));
        false ->
            erlfdb:wait(erlfdb:set(Tx, EncKey, V))
    end,
    erlfdb:wait(erlfdb:commit(Tx));
put(#st{db = Db, mtab = {_, index, {KeyPos, _}}, index = Indexes}, {V0, K}, {[]}) ->
    ?dbg("Adding data index: ~p ~p ~p", [K, V0, Indexes]),
    %% indexes handled in data put
    case element(KeyPos, Indexes) of
        undefined ->
            ok;
        Idx ->
            Tx = erlfdb:create_transaction(Db),
            ok = add_data_indexes_(Tx, K, V0, Idx),
            ok = erlfdb:wait(erlfdb:commit(Tx))
    end,
    ok.

add_data_indexes_(Tx, K, Val, #idx{table_id = TableId}) ->
    EncKey = idx_count_key(TableId, Val),
    {ok, _} = update_counter(Tx, EncKey, 1),
    ?dbg("Add data index ~p", [{<<"di">>, {Val, K}}]),
    ok = erlfdb:wait(erlfdb:set(Tx, encode_key(TableId, {<<"di">>, {Val, K}}), <<>>)).

remove_data_indexes_(bag, Tx, Val, Key, #idx{table_id = TableId}) ->
    EncKey = idx_count_key(TableId, Val),
    Cnt = length(erlfdb:wait(erlfdb:get_range_startswith(Tx, encode_prefix(TableId, {<<"bi">>, Val, {Key, '_'}})))),
    {ok, _} = update_counter(Tx, EncKey, Cnt * -1),
    ?dbg("Remove data index ~p", [{<<"bi">>, Val, '_'}]),
    ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, encode_prefix(TableId, {<<"bi">>, Val, {Key, '_'}})));
remove_data_indexes_(_Type, Tx, Val, Key, #idx{table_id = TableId}) ->
    EncKey = idx_count_key(TableId, Val),
    {ok, _} = update_counter(Tx, EncKey, -1),
    ?dbg("Remove data index ~p", [{<<"di">>, {Val, Key}}]),
    ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, encode_prefix(TableId, {<<"di">>, {Val, Key}}))).

put_bag_(Tx, TableId, MTab, HcaBag, HcaRef, _Indexes, {NewKey, NewVal0})
  when is_atom(MTab) ->
    NewEncVal = encode_value(NewVal0),
    <<NewValSize:32, _/binary>> = NewEncVal,
    %% Add size header as first 32-bits of value
    case bag_get_(Tx, TableId, NewKey) of
        [] ->
            ?dbg("Adding new Key ~p to bag ~p", [NewKey, MTab]),
            HcaVal = erlfdb_hca:allocate(HcaBag, Tx),
            EncKey = encode_key(TableId, {<<"b">>, NewKey, HcaVal}),
            case NewValSize > ?MAX_VALUE_SIZE of
                true ->
                    MfdbRefPartId = save_parts(Tx, TableId, HcaRef, NewEncVal),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", NewValSize:32, MfdbRefPartId/binary>>));
                false ->
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, NewEncVal))
            end,
            ok = erlfdb:wait(erlfdb:add(Tx, tbl_size_key(TableId), NewValSize));
        PossMatches ->
            ?dbg("Found matches in bag [~p]: ~p", [MTab, PossMatches]),
            ok = bag_maybe_add_(PossMatches,
                                Tx, TableId, MTab,
                                HcaBag, HcaRef, NewValSize,
                                NewKey, NewVal0, NewEncVal)
    end;
put_bag_(Tx, IdxTableId, {Parent, index, {KeyPos, _}}, _HcaBag, _HcaRef, Indexes, {{Val, Key}, {[]}} = KV) ->
    ?dbg("put bag index: ~p", [KV]),
    %% Ridiculously expensive as we have to
    %% get the value of KeyPos for all matching keys to check
    %% whether or not an index already exists
    case element(KeyPos, Indexes) of
        undefined ->
            ok;
        #idx{table_id = IdxTableId} ->
            %% We need the TableId of the parent
            #st{table_id = DataTableId} = mfdb_manager:st(Parent),
            case bag_get_(Tx, DataTableId, Key) of
                [] ->
                    ok;
                DataMatches ->
                    bag_maybe_add_index(Tx, DataTableId, IdxTableId, KeyPos, Val, DataMatches)
            end
    end.

bag_maybe_add_index(Tx, DataTableId, IdxTableId, KeyPos, Val, [{BagKey0, BagVal} | Rest]) ->
    Size = bit_size(DataTableId),
    <<DataTableId:Size/bits, DataKey/binary>> = BagKey0,
    {<<"b">>, BiK, BiHca} = sext:decode(DataKey),
    BagKey = {BiK, BiHca},
    BVal = decode_val(Tx, DataTableId, BagVal),
    CompVal = element(KeyPos, BVal),
    case CompVal =:= Val of
        false ->
            %% Value does not match value for index
            ok;
        true ->
            %% Value matches value for index
            %% Check if we already have an index entry, since we
            %% don't want to mess up bag index value counters
            EncIdxKey = encode_key(IdxTableId, {<<"bi">>, Val, BagKey}),
            case erlfdb:wait(erlfdb:get(Tx, EncIdxKey)) of
                not_found ->
                    EncIdxCountKey = idx_count_key(IdxTableId, Val),
                    {ok, _} = update_counter(Tx, EncIdxCountKey, 1),
                    ?dbg("Add bag index ~p", [EncIdxKey]),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncIdxKey, <<>>));
                _ ->
                    %% We already had an index entry
                    ok
            end
    end,
    bag_maybe_add_index(Tx, DataTableId, IdxTableId, KeyPos, Val, Rest).

bag_maybe_add_([], Tx, TableId, MTab,
               HcaBag, HcaRef, Size,
               NewKey, NewVal, NewEncVal)
  when is_atom(MTab) ->
    %% We did not have a matching value, so add
    ?dbg("No matching values set so add: ~p ~p", [NewKey, NewVal]),
    HcaVal = erlfdb_hca:allocate(HcaBag, Tx),
    EncKey = encode_key(TableId, {<<"b">>, NewKey, HcaVal}),
    ok = bag_set_(Tx, TableId, EncKey, NewEncVal, HcaRef, Size),
    ok = erlfdb:wait(erlfdb:add(Tx, tbl_size_key(TableId), Size));
bag_maybe_add_([{_EncKey, <<"mfdb_ref", _OldSize:32, MfdbRefPartId/binary>>} | Rest],
               Tx, TableId, MTab,
               HcaBag, HcaRef, Size,
               NewKey, NewVal, NewEncVal)
  when is_atom(MTab) andalso Size > ?MAX_VALUE_SIZE ->
    %% References large record with multiple parts
    OldVal = parts_value_(MfdbRefPartId, TableId, Tx),
    case OldVal =:= NewEncVal of
        true ->
            ?dbg("Bag large value already exists: ~p", [NewKey]),
            %% Same key and value, noop
            %% We don't bother replacing when
            %% a bag item has the same value
            ok;
        false ->
            bag_maybe_add_(Rest, Tx, TableId, MTab,
                           HcaBag, HcaRef, Size,
                           NewKey, NewVal, NewEncVal)
    end;
bag_maybe_add_([{_EncKey, EncVal} | Rest],
               Tx, TableId, MTab, HcaBag, HcaRef, Size,
               NewKey, NewVal, NewEncVal)
  when is_atom(MTab) ->
    case EncVal =:= NewEncVal of
        true ->
            ?dbg("Bag [~p] small value already exists: ~p", [MTab, NewKey]),
            %% noop. We don't do anything when a bag
            %% item has the same key and value
            ok;
        false ->
            bag_maybe_add_(Rest, Tx, TableId, MTab,
                           HcaBag, HcaRef, Size,
                           NewKey, NewVal, NewEncVal)
    end.

parts_value_(MfdbRefPartId, TableId, Tx) ->
    {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
    Start = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
    Parts = erlfdb:wait(erlfdb:get_range_startswith(Tx, Start)),
    bin_join_parts(Parts).

bag_get_(Tx, TableId, Key) ->
    StartKey = encode_prefix(TableId, {<<"b">>, Key, '_'}),
    erlfdb:wait(erlfdb:get_range(Tx, StartKey, erlfdb_key:strinc(StartKey))).

bag_set_(Tx, TableId, EncKey, EncVal, HcaRef, Size) ->
    case Size > ?MAX_VALUE_SIZE of
        true ->
            MfdbRefPartId = save_parts(Tx, TableId, HcaRef, EncVal),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", Size:32, MfdbRefPartId/binary>>));
        false ->
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, EncVal))
    end.

delete(#st{db = Db, type = Type, mtab = {_, index, {KeyPos, _}}, index = Indexes}, {V0, Key}) ->
    %% noop - indexes handled in data handlers
    ?dbg("Delete data index: {~p, ~p}", [V0, Key]),
    case element(KeyPos, Indexes) of
        undefined ->
            ok;
        Idx ->
            Tx = erlfdb:create_transaction(Db),
            ok = remove_data_indexes_(Type, Tx, V0, Key, Idx),
            ok = erlfdb:wait(erlfdb:commit(Tx))
    end;
delete(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, index = Indexes, type = bag}, K) ->
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    Tx = erlfdb:create_transaction(Db),
    R = do_delete_bag(Tx, TableId, MTab, tuple_to_list(Indexes), K),
    erlfdb:wait(erlfdb:commit(Tx)),
    R;
delete(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab}, K) when is_atom(MTab) ->
    %% deleting a data item
    Tx = erlfdb:create_transaction(Db),
    do_delete_data_(Tx, TableId, true, K);
delete(#st{db = ?IS_TX = Tx, table_id = TableId}, K) ->
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    do_delete_data_(Tx, TableId, false, K).

do_delete_data_(Tx, TableId, DoCommit, K) ->
    EncKey = encode_key(TableId, {<<"d">>, K}),
    case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            ok;
        <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> ->
            %% decrement size
            erlfdb:wait(erlfdb:add(Tx, tbl_size_key(TableId), OldSize * -1)),
            %% Remove parts of large value
            {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
            Start = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
            ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
            %% decrement item count
            erlfdb:add(Tx, tbl_count_key(TableId), -1);
        <<OldSize:32, _EncVal/binary>> ->
            %% decrement size
            erlfdb:add(Tx, tbl_size_key(TableId), OldSize * -1),
            %% decrement item count
            erlfdb:add(Tx, tbl_count_key(TableId), -1)
    end,
    ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
    case DoCommit of
        true ->
            ok = erlfdb:wait(erlfdb:commit(Tx));
        false ->
            ok
    end.

do_delete_bag(Tx, TableId, MTab, Indexes, K) when is_atom(MTab) ->
    StartKey = encode_prefix(TableId, {<<"b">>, K, '_'}),
    do_delete_bag_(Tx, TableId, Indexes, StartKey).

do_delete_bag_(?IS_TX = Tx, TableId, Indexes, StartKey) ->
    %% IMPORTANT: We do *not* commit the transaction here
    %% as the caller passed in an existing transaction
    %% and they are responsible for the commit
    DelFun = fun({EncKey, <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>>}, Cnt) ->
                     {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
                     ClearPrefix = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
                     ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, ClearPrefix)),
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     {ok, _} = update_counter(Tx, tbl_size_key(TableId), OldSize * -1),
                     {ok, _} = update_counter(Tx, tbl_count_key(TableId), -1),
                     Cnt + 1;
                ({EncKey, <<OldSize:32, _EncVal0/binary>>}, Cnt) ->
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     {ok, _} = update_counter(Tx, tbl_size_key(TableId), OldSize * -1),
                     {ok, _} = update_counter(Tx, tbl_count_key(TableId), -1),
                     Cnt + 1
             end,
    try erlfdb:wait(
          erlfdb:fold_range(
            Tx,
            StartKey,
            erlfdb_key:strinc(StartKey),
            DelFun,
            0,
            [{limit, 100}])) of
        Removed when Removed < 100 ->
            ok;
        Removed when Removed > 99 ->
            do_delete_bag_(Tx, TableId, Indexes, StartKey)
    catch
        FoldErr:FoldErrMsg ->
            lager:error("Error with in deleting bag entries: ~p ~p",
                        [FoldErr, FoldErrMsg]),
            {error, FoldErrMsg}
    end.

idx_matches(#st{db = DbOrTx, index = Indexes}, IdxPos, Key0) ->
    #idx{table_id = TableId} = element(IdxPos, Indexes),
    Key = idx_count_key(TableId, Key0),
    R = case DbOrTx of
            ?IS_DB ->
                erlfdb:get(DbOrTx, Key);
            ?IS_TX ->
                erlfdb:wait(erlfdb:get(DbOrTx, Key))
        end,
    case R of
        not_found ->
            0;
        <<Count:64/unsigned-little-integer>> ->
            Count
    end.

table_data_size(#st{db = DbOrTx, table_id = TableId}) ->
    Key = tbl_size_key(TableId),
    R = case DbOrTx of
            ?IS_DB ->
                erlfdb:get(DbOrTx, Key);
            ?IS_TX ->
                erlfdb:wait(erlfdb:get(DbOrTx, Key))
        end,
    case R of
        not_found ->
            0;
        <<Count:64/unsigned-little-integer>> ->
            %% Convert byes to words
            erlang:round(Count / erlang:system_info(wordsize))
    end.

table_count(#st{db = DbOrTx, table_id = TableId}) ->
    Key = tbl_count_key(TableId),
    R = case DbOrTx of
            ?IS_DB ->
                erlfdb:get(DbOrTx, Key);
            ?IS_TX ->
                erlfdb:wait(erlfdb:get(DbOrTx, Key))
        end,
    case R of
        not_found ->
            0;
        <<Count:64/unsigned-little-integer>> ->
            Count
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

decode_key(TableId, <<S:8, R/binary>>) ->
    <<TableId:S/bits, Bin/binary>> = R,
    case sext:decode(Bin) of
        {<<"bi">>, Val, {Id, _HcaVal}} ->
            %% bag index reference key
            {idx, {{Val, Id}}};
        {<<"di">>, {Val, Id}} ->
            %% data index reference key
            {idx, {{Val, Id}}};
        {<<"d">>, Key} ->
            Key;
        {<<"i">>, Key} ->
            {cnt_idx, Key};
        {<<"b">>, Key, _HcaVal} ->
            Key;
        {<<"p">>, PartHcaVal} ->
            PartHcaVal;
        BadVal ->
            exit({TableId, BadVal})
    end.

decode_bag_key(TableId, <<S:8, R/binary>>) ->
    <<TableId:S/bits, Bin/binary>> = R,
    {<<"b">>, Key, HcaVal} = sext:decode(Bin),
    {Key, HcaVal}.

decode_val(_Db, _TableId, <<>>) ->
    <<>>;
decode_val(Db, TableId, <<"mfdb_ref", _OldSize:32, MfdbRefPartId/binary>>) ->
    PartsVal = parts_value_(MfdbRefPartId, TableId, Db),
    <<_:32, Val>> = bin_join_parts(PartsVal),
    binary_to_term(Val);
decode_val(_Db, _TableId, <<_:32, CodedVal/binary>>) ->
    binary_to_term(CodedVal).

encode_key(TableId, Key) ->
    <<(bit_size(TableId)):8, TableId/binary, (sext:encode(Key))/binary>>.

encode_prefix(TableId, Key) ->
    <<(bit_size(TableId)):8, TableId/binary, (sext:prefix(Key))/binary>>.

encode_value(Val0) ->
    Val = term_to_binary(Val0),
    Size = byte_size(Val),
    <<Size:32, Val/binary>>.

tbl_count_key(TableId) ->
    encode_key(TableId, {<<"c">>}).

tbl_size_key(TableId) ->
    encode_key(TableId, {<<"s">>}).

idx_count_key(TableId, Value) ->
    encode_key(TableId, {<<"i">>, Value}).

save_parts(?IS_TX = Tx, TableId, Hca, Bin) ->
    PartId = erlfdb_hca:allocate(Hca, Tx),
    PartKey = sext:encode({<<"p">>, PartId}),
    ok = save_parts_(Tx, TableId, PartId, 0, Bin),
    PartKey.

save_parts_(_Tx, _TableId, _PartId, _PartInc, <<>>) ->
    ok;
save_parts_(Tx, TableId, PartId, PartInc, <<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>) ->
    Key = encode_key(TableId, {<<"p">>, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Part)),
    save_parts_(Tx, TableId, PartId, PartInc + 1, Rest);
save_parts_(Tx, TableId, PartId, PartInc, Tail) ->
    Key = encode_key(TableId, {<<"p">>, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Tail)),
    save_parts_(Tx, TableId, PartId, PartInc + 1, <<>>).

update_counter(Tx, EncKey, Incr) ->
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
    Old = case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
              not_found ->
                  erlfdb:wait(erlfdb:add(Tx, EncKey, 0)),
                  0;
              <<OldVal:64/unsigned-little-integer>> ->
                  OldVal
          end,
    case Old + Incr of
        N when N < 0 ->
            %% Set counter to zero and return zero
            erlfdb:wait(erlfdb:set(Tx, EncKey, <<0:64/unsigned-little-integer>>)),
            {ok, 0};
        _N ->
            %% Counter incremented, return new value
            %% This could very well not be what was expected
            %% since the counter may have been incremented
            %% by other transactions
            ok = erlfdb:wait(erlfdb:add(Tx, EncKey, Incr)),
            NewVal = erlfdb:wait(erlfdb:get(Tx, EncKey)),
            {ok, NewVal}
    end.
