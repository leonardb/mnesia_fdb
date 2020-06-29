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
         table_size/1]).

-include("mfdb.hrl").

put(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, hca_bag = HcaBag, hca_ref = HcaRef, index = Indexes, type = bag}, K, V) ->
    put_bag_(Db, TableId, MTab, HcaBag, HcaRef, tuple_to_list(Indexes), [{K, V}]);
put(#st{db = ?IS_TX = Tx, table_id = TableId, mtab = MTab, hca_bag = HcaBag, hca_ref = HcaRef, index = Indexes, type = bag}, K, V) ->
    put_bag_(Tx, TableId, MTab, HcaBag, HcaRef, tuple_to_list(Indexes), [{K, V}]);
put(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab, hca_ref = HcaRef}, K, V0) when is_atom(MTab) ->
    %% Operation is on a data table
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
put(#st{db = ?IS_DB = Db, table_id = TableId, mtab = {_, index, _}}, {KeyValue, _RefId} = K, {[]}) ->
    %% Operation is on an index
    EncKey = encode_key(TableId, {<<"di">>, K}),
    V = <<>>,
    Tx = erlfdb:create_transaction(Db),
    case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            %% New entry, so increment index value count
            ok = erlfdb:wait(erlfdb:add(Tx, idx_count_key(TableId, KeyValue), 1));
        _ ->
            ok
    end,
    erlfdb:wait(erlfdb:set(Tx, EncKey, V)),
    erlfdb:wait(erlfdb:commit(Tx)).

put_bag_(_Tx, _TableId, _MTab, _HcaBag, _HcaRef, _Indexes, []) ->
    ok;
put_bag_(Tx, TableId, MTab, HcaBag, HcaRef, Indexes, [{NewKey, NewVal0} | Rest])
  when is_atom(MTab) ->
    NewEncVal = encode_value(NewVal0),
    <<NewValSize:32, _/binary>> = NewEncVal,
    %% Add size header as first 32-bits of value
    case bag_get_(Tx, TableId, NewKey) of
        [] ->
            HcaVal = erlfdb_hca:allocate(HcaBag, Tx),
            EncKey = encode_key(TableId, {<<"b">>, NewKey, HcaVal}),
            case NewValSize > ?MAX_VALUE_SIZE of
                true ->
                    MfdbRefPartId = save_parts(Tx, TableId, HcaRef, NewEncVal),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", NewValSize:32, MfdbRefPartId/binary>>));
                false ->
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, NewEncVal))
            end,
            ok = erlfdb:wait(erlfdb:add(Tx, tbl_size_key(TableId), NewValSize)),
            update_bag_indexes_(Tx, NewKey, NewVal0, HcaVal, 1, Indexes);
        PossMatches ->
            ?dbg("Found matches in bag: ~p", [PossMatches]),
            ok = bag_maybe_add_(PossMatches,
                                Tx, TableId, MTab,
                                HcaBag, HcaRef, Indexes,
                                NewValSize, NewKey, NewVal0, NewEncVal)
    end,
    put_bag_(Tx, TableId, MTab, HcaBag, HcaRef, Indexes, Rest);
put_bag_(_Tx, _TableId, {_, index, _} = _MTab, _HcaBag, _HcaRef, _Indexes, _KVs) ->
    %% Skip, we maintain our own indexes
    ok.

update_bag_indexes_(_DbOrTx, _K, _V0, _HcaVal, _Inc, []) ->
    ok;
update_bag_indexes_(DbOrTx, K, V0, HcaVal, Inc, [undefined | Rest]) ->
    update_bag_indexes_(DbOrTx, K, V0, HcaVal, Inc, Rest);
update_bag_indexes_(DbOrTx, K, V0, HcaVal, Inc, [#idx{table_id = TableId, pos = KPos} | Rest]) ->
    ?dbg("Update bag index for ~p ~p", [K, V0]),
    KeyValue = element(KPos, V0),
    IdxKey = encode_key(TableId, {<<"bi">>, KeyValue, {K, HcaVal}}),
    V = <<>>,
    GetRes = db_or_tx_exec_(DbOrTx, fun() -> erlfdb:get(DbOrTx, IdxKey) end),
    case GetRes of
        not_found ->
            %% Add the index entry
            ok = db_or_tx_exec_(DbOrTx, fun() -> erlfdb:set(DbOrTx, IdxKey, V) end),
            %% New entry, so increment index value count
            ok = db_or_tx_exec_(DbOrTx, fun() -> erlfdb:add(DbOrTx, idx_count_key(TableId, KeyValue), Inc) end);
        _ ->
            ok
    end,
    update_bag_indexes_(DbOrTx, K, V0, HcaVal, Inc, Rest).

bag_maybe_add_([], Tx, TableId, MTab,
               HcaBag, HcaRef, Indexes,
               Size, NewKey, NewVal, NewEncVal)
  when is_atom(MTab) ->
    %% We did not have a matching value, so add
    ?dbg("No matching values set so add: ~p ~p", [NewKey, NewVal]),
    HcaVal = erlfdb_hca:allocate(HcaBag, Tx),
    EncKey = encode_key(TableId, {<<"b">>, NewKey, HcaVal}),
    ok = bag_set_(Tx, TableId, EncKey, NewEncVal, HcaRef, Size),
    ok = erlfdb:wait(erlfdb:add(Tx, tbl_size_key(TableId), Size)),
    update_bag_indexes_(Tx, NewKey, NewVal, HcaVal, 1, Indexes);
bag_maybe_add_([{_EncKey, <<"mfdb_ref", _OldSize:32, MfdbRefPartId/binary>>} | Rest],
               Tx, TableId, MTab,
               HcaBag, HcaRef, Indexes,
               Size, NewKey, NewVal, NewEncVal)
  when is_atom(MTab) andalso Size > ?MAX_VALUE_SIZE ->
    %% References large record with multiple parts
    OldVal = bag_parts_value_(MfdbRefPartId, TableId, Tx),
    case OldVal =:= NewEncVal of
        true ->
            ?dbg("Bag large value already exists: ~p", [NewKey]),
            %% Same key and value, noop
            %% We don't bother replacing when
            %% a bag item has the same value
            ok;
        false ->
            bag_maybe_add_(Rest, Tx, TableId, MTab,
                           HcaBag, HcaRef, Indexes,
                           Size, NewKey, NewVal, NewEncVal)
    end;
bag_maybe_add_([{_EncKey, EncVal} | Rest],
               Tx, TableId, MTab, HcaBag, HcaRef, Indexes,
               Size, NewKey, NewVal, NewEncVal)
  when is_atom(MTab) ->
    case EncVal =:= NewEncVal of
        true ->
            ?dbg("Bag small value already exists: ~p", [NewKey]),
            %% noop. We don't do anything when a bag
            %% item has the same key and value
            ok;
        false ->
            bag_maybe_add_(Rest, Tx, TableId, MTab,
                           HcaBag, HcaRef, Indexes,
                           Size, NewKey, NewVal, NewEncVal)
    end.

bag_parts_value_(MfdbRefPartId, TableId, Tx) ->
    {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
    Start = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
    Parts = db_or_tx_exec_(Tx, fun() -> erlfdb:get_range_startswith(Tx, Start) end),
    bin_join_parts(Parts).

bag_get_(DbOrTx, TableId, Key) ->
    StartKey = encode_prefix(TableId, {<<"b">>, Key, '_'}),
    db_or_tx_exec_(DbOrTx, fun() -> erlfdb:get_range(DbOrTx, StartKey, erlfdb_key:strinc(StartKey)) end).

bag_set_(Tx, TableId, EncKey, EncVal, HcaRef, Size) ->
    case Size > ?MAX_VALUE_SIZE of
        true ->
            MfdbRefPartId = save_parts(Tx, TableId, HcaRef, EncVal),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", Size:32, MfdbRefPartId/binary>>));
        false ->
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, EncVal))
    end.

delete(#st{db = DbOrTx, table_id = TableId, mtab = MTab, index = Indexes, type = bag}, K) ->
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    do_delete_bag(DbOrTx, TableId, MTab, Indexes, K);
delete(#st{db = ?IS_DB = Db, table_id = TableId, mtab = MTab}, K) when is_atom(MTab) ->
    %% deleting a data item
    EncKey = encode_key(TableId, {<<"d">>, K}),
    case erlfdb:get(Db, EncKey) of
        not_found ->
            ok;
        <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> ->
            %% decrement size
            erlfdb:add(Db, tbl_size_key(TableId), OldSize * -1),
            %% Remove parts of large value
            {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
            Start = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
            ok = erlfdb:clear_range_startswith(Db, Start);
        <<OldSize:32, _/binary>> ->
            %% decrement size
            erlfdb:add(Db, tbl_size_key(TableId), OldSize * -1),
            ok
    end,
    %% decrement item count
    erlfdb:add(Db, tbl_count_key(TableId), -1),
    ok = erlfdb:clear(Db, EncKey);
delete(#st{db = ?IS_DB = Db, table_id = TableId, mtab = {_, index, _}}, {KeyVal, _KeyRef} = Key) ->
    %% deleting an index item
    EncKey = encode_key(TableId, {<<"d">>, Key}),
    case erlfdb:get(Db, EncKey) of
        not_found ->
            ok;
        _ ->
            %% decrement count of index items for value
            erlfdb:add(Db, idx_count_key(TableId, KeyVal), -1),
            ok
    end,
    ok = erlfdb:clear(Db, EncKey);
delete(#st{db = ?IS_TX = Tx, table_id = TableId}, K) ->
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    EncKey = encode_key(TableId, {<<"d">>, K}),
    case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            ok;
        <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> ->
            %% decrement size
            erlfdb:add(Tx, tbl_size_key(TableId), OldSize * -1),
            %% Remove parts of large value
            {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
            Start = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
            ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start));
        <<OldSize:32, _EncVal/binary>> ->
            %% decrement size
            erlfdb:add(Tx, tbl_size_key(TableId), OldSize * -1),
            ok
    end,
    %% decrement item count
    erlfdb:add(Tx, tbl_count_key(TableId), -1),
    ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)).

do_delete_bag(_DbOrTx, _TableId, {_, index, _}, _Indexes, _K) ->
    %% we handle Bag indexes in the data put/delete operations
    ok;
do_delete_bag(DbOrTx, TableId, MTab, Indexes, K) when is_atom(MTab) ->
    StartKey = encode_prefix(TableId, {<<"b">>, K, '_'}),
    case do_delete_bag_(DbOrTx, TableId, Indexes, StartKey) of
        K ->
            ok;
        [] ->
            ok;
        NextKey ->
            do_delete_bag(DbOrTx, TableId, MTab, Indexes, NextKey)
    end.

do_delete_bag_(?IS_DB = Db, TableId, Indexes, StartKey) ->
    Tx = erlfdb:create_transaction(Db),
    DelFun = fun({EncKey, <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> = Ref}, _LastKey) ->
                     {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
                     V0 = decode_val(Tx, TableId, Ref),
                     ClearPrefix = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
                     ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, ClearPrefix)),
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     erlfdb:add(Db, tbl_size_key(TableId), OldSize * -1),
                     erlfdb:add(Tx, tbl_count_key(TableId), -1),
                     {K, HcaVal} = decode_bag_key(TableId, EncKey),
                     ok = update_bag_indexes_(Tx, K, V0, HcaVal, -1, Indexes),
                     decode_key(TableId, EncKey);
                ({EncKey, <<OldSize:32, EncVal0/binary>>}, _LastKey) ->
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     erlfdb:add(Db, tbl_size_key(TableId), OldSize * -1),
                     erlfdb:add(Tx, tbl_count_key(TableId), -1),
                     V0 = binary_to_term(EncVal0),
                     {K, HcaVal} = decode_bag_key(TableId, EncKey),
                     ok = update_bag_indexes_(Tx, K, V0, HcaVal, -1, Indexes),
                     decode_key(TableId, EncKey)
             end,
    try erlfdb:wait(erlfdb:fold_range(Tx, StartKey, erlfdb_key:strinc(StartKey), DelFun, [], [{limit, 100}])) of
        NextKey ->
            ok = erlfdb:wait(erlfdb:commit(Tx)),
            NextKey
    catch
        FoldErr:FoldErrMsg ->
            ok = erlfdb:wait(erlfdb:reset(Tx)),
            lager:error("Error with in deleting bag entries: ~p ~p",
                        [FoldErr, FoldErrMsg]),
            {error, FoldErrMsg}
    end;
do_delete_bag_(?IS_TX = Tx, TableId, Indexes, StartKey) ->
    %% IMPORTANT: We do *not* commit the transaction here
    %% as the caller passed in an existing transaction
    %% and they are responsible for the commit
    DelFun = fun({EncKey, <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> = Ref}, _LastKey) ->
                     {<<"p">>, PartHcaVal} = sext:decode(MfdbRefPartId),
                     V0 = decode_val(Tx, TableId, Ref),
                     ClearPrefix = encode_prefix(TableId, {<<"p">>, PartHcaVal, <<"_">>, '_'}),
                     ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, ClearPrefix)),
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     erlfdb:add(Tx, tbl_size_key(TableId), OldSize * -1),
                     erlfdb:add(Tx, tbl_count_key(TableId), -1),
                     {K, HcaVal} = decode_bag_key(TableId, EncKey),
                     ok = update_bag_indexes_(Tx, K, V0, HcaVal, -1, Indexes),
                     decode_key(TableId, EncKey);
                ({EncKey, <<OldSize:32, EncVal0/binary>>}, _LastKey) ->
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     erlfdb:add(Tx, tbl_size_key(TableId), OldSize * -1),
                     erlfdb:add(Tx, tbl_count_key(TableId), -1),
                     V0 = binary_to_term(EncVal0),
                     {K, HcaVal} = decode_bag_key(TableId, EncKey),
                     ok = update_bag_indexes_(Tx, K, V0, HcaVal, -1, Indexes),
                     decode_key(TableId, EncKey)
             end,
    try erlfdb:wait(
          erlfdb:fold_range(
            Tx,
            StartKey,
            erlfdb_key:strinc(StartKey),
            DelFun,
            [],
            [{limit, 100}])) of
        NextKey ->
            NextKey
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

table_size(#st{db = DbOrTx, table_id = TableId}) ->
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
            Count
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
            {i, Val, Id};
        {<<"di">>, {Val, Id}} ->
            %% data index reference key
            {i, Val, Id};
        {<<"d">>, Key} ->
            {d, Key};
        {<<"b">>, Key, _HcaVal} ->
            {d, Key};
        {<<"p">>, PartHcaVal} ->
            {p, PartHcaVal};
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
    PartsVal = bag_parts_value_(MfdbRefPartId, TableId, Db),
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

db_or_tx_exec_(?IS_DB, Fun) ->
    Fun();
db_or_tx_exec_(?IS_TX, Fun) ->
    erlfdb:wait(Fun()).
