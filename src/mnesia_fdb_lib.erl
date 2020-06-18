%%% @doc RocksDB update  wrappers, in separate module for easy tracing and mocking.
%%%
-module(mnesia_fdb_lib).

-compile({no_auto_import, [put/2, get/1]}).

-export([put/2,
         put/3,
         delete/2,
         bin_split/1,
         bin_join/1,
         decode_key/2,
         decode_val/2,
         encode_key/2,
         encode_prefix/2,
         save_parts/4]).

-include("mnesia_fdb.hrl").

put(#st{db = ?IS_DB = Db, table_id = TableId, hca_bag = HcaBag, hca_ref = HcaRef, type = bag}, KVs)
  when is_list(KVs) ->
    Tx = erlfdb:create_transaction(Db),
    ok = put_bag_(Tx, TableId, HcaBag, HcaRef, KVs),
    erlfdb:wait(erlfdb:commit(Tx));
put(#st{db = ?IS_TX = Tx, table_id = TableId, hca_bag = HcaBag, hca_ref = HcaRef, type = bag}, KVs)
  when is_list(KVs) ->
    ok = put_bag_(Tx, TableId, HcaBag, HcaRef, KVs);
put(#st{db = ?IS_DB = Db, table_id = TableId, hca_ref = HcaRef, type = Type}, KVs)
  when (Type =:= set orelse Type =:= ordered_set) andalso is_list(KVs) ->
    Tx = erlfdb:create_transaction(Db),
    put_(Tx, TableId, HcaRef, KVs),
    erlfdb:wait(erlfdb:commit(Tx));
put(#st{db = ?IS_TX = Tx, table_id = TableId, hca_ref = HcaRef, type = Type}, KVs)
  when (Type =:= set orelse Type =:= ordered_set) andalso is_list(KVs) ->
    ok = put_(Tx, TableId, HcaRef, KVs).

put_(_Tx, _TableId, _HcaRef, []) ->
    ok;
put_(Tx, TableId, HcaRef, [{K, V0} | Rest]) ->
    EncKey = encode_key(TableId, {<<"d">>, K}),
    V = term_to_binary(V0),
    case byte_size(V) > ?MAX_VALUE_SIZE of
        true ->
            %% Ensure we remove any old parts
            case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
                <<"mfdb_ref", OldMfdbRefPrefix/binary>> ->
                    erlfdb:wait(erlfdb:clear_range_startswith(Tx, OldMfdbRefPrefix));
                _ ->
                    ok
            end,
            %% Save the new parts
            MfdbRefPrefix = save_parts(Tx, TableId, HcaRef, V),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", MfdbRefPrefix/binary>>)),
            erlfdb:wait(erlfdb:commit(Tx));
        false ->
            erlfdb:wait(erlfdb:set(Tx, EncKey, V))
    end,
    put_(Tx, TableId, HcaRef, Rest).

put_bag_(_Tx, _TableId, _HcaBag, _HcaRef, []) ->
    ok;
put_bag_(Tx, TableId, HcaBag, HcaRef, [{K, V0} | Rest]) ->
    StartKey = encode_key(TableId, {<<"b">>, K}),
    V = term_to_binary(V0),
    case erlfdb:wait(erlfdb:get_range(Tx, StartKey, erlfdb_key:strinc(StartKey))) of
        [] ->
            EncKey = encode_key(TableId, {<<"b">>, K, erlfdb_hca:allocate(HcaBag, Tx)}),
            case byte_size(V) > ?MAX_VALUE_SIZE of
                true ->
                    MfdbRefPrefix = save_parts(Tx, TableId, HcaRef, V),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", MfdbRefPrefix/binary>>));
                false ->
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, V))
            end;
        PossMatches ->
            ok = bag_maybe_replace_(PossMatches, Tx, TableId, HcaBag, HcaRef, {K, V})
    end,
    put_bag_(Tx, TableId, HcaBag, HcaRef, Rest).

bag_maybe_replace_([], Tx, TableId, HcaBag, HcaRef, {K, V}) ->
    %% We did not have a matching value, so add
    EncKey = encode_key(TableId, {<<"b">>, K, erlfdb_hca:allocate(HcaBag, Tx)}),
    case byte_size(V) > ?MAX_VALUE_SIZE of
        true ->
            MfdbRefPrefix = save_parts(Tx, TableId, HcaRef, V),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", MfdbRefPrefix/binary>>));
        false ->
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, V))
    end;
bag_maybe_replace_([{_EncKey, <<"mfdb_ref", MfdbPrefix/binary>>} | Rest], Tx, TableId, HcaBag, HcaRef, {_K, V} = KV) ->
    %% References large record with multiple parts
    Parts = erlfdb:wait(erlfdb:get_range_startswith(Tx, MfdbPrefix)),
    case bin_join(Parts) =:= V of
        true ->
            %% noop. We don't bother replacing when a bag item has the same value
            ok;
        false ->
            bag_maybe_replace_(Rest, Tx, TableId, HcaBag, HcaRef, KV)
    end;
bag_maybe_replace_([{_EncKey, EncVal} | Rest], Tx, TableId, HcaBag, HcaRef, {_K, V} = KV) ->
    case EncVal =:= V of
        true ->
            %% noop. We don't bother replacing when a bag item has the same value
            ok;
        false ->
            bag_maybe_replace_(Rest, Tx, TableId, HcaBag, HcaRef, KV)
    end.

put(#st{db = ?IS_DB = Db, table_id = TableId, hca_bag = HcaBag, hca_ref = HcaRef, type = bag}, K, V) ->
    put_bag_(Db, TableId, HcaBag, HcaRef, [{K, V}]);
put(#st{db = ?IS_TX = Tx, table_id = TableId, hca_bag = HcaBag, hca_ref = HcaRef, type = bag}, K, V) ->
    put_bag_(Tx, TableId, HcaBag, HcaRef, [{K, V}]);
put(#st{db = ?IS_DB = Db, table_id = TableId, hca_ref = HcaRef}, K, V0) ->
    EncKey = encode_key(TableId, {<<"d">>, K}),
    V = term_to_binary(V0),
    case byte_size(V) > ?MAX_VALUE_SIZE of
        true ->
            %% Ensure we remove any old parts
            Tx = erlfdb:create_transaction(Db),
            case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
                <<"mfdb_ref", OldMfdbRefPrefix/binary>> ->
                    erlfdb:wait(erlfdb:clear_range_startswith(Tx, OldMfdbRefPrefix));
                _ ->
                    ok
            end,
            %% Save the new parts
            MfdbRefPrefix = save_parts(Tx, TableId, HcaRef, V),
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", MfdbRefPrefix/binary>>)),
            erlfdb:wait(erlfdb:commit(Tx));
        false ->
            erlfdb:set(Db, EncKey, V)
    end.

delete(#st{db = DbOrTx, table_id = TableId, type = bag}, K) ->
    StartKey = encode_key(TableId, {<<"b">>, K}),
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    do_delete_bag(DbOrTx, StartKey);
delete(#st{db = ?IS_DB = Db, table_id = TableId}, K) ->
    EncKey = encode_key(TableId, {<<"d">>, K}),
    case erlfdb:get(Db, EncKey) of
        not_found ->
            ok;
        <<"mfdb_ref", MfdbPrefix/binary>> ->
            %% Remove parts of large value
            ok = erlfdb:clear_range_startswith(Db, MfdbPrefix);
        _EncVal ->
            ok
    end,
    ok = erlfdb:clear(Db, EncKey);
delete(#st{db = ?IS_TX = Tx, table_id = TableId}, K) ->
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    EncKey = encode_key(TableId, {<<"d">>, K}),
    case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            ok;
        <<"mfdb_ref", MfdbPrefix/binary>> ->
            %% Remove parts of large value
            ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, MfdbPrefix));
        _EncVal ->
            ok
    end,
    ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)).

do_delete_bag(DbOrTx, StartKey) ->
    case do_delete_bag_(DbOrTx, StartKey) of
        StartKey ->
            ok;
        [] ->
            ok;
        NextKey ->
            do_delete_bag(DbOrTx, NextKey)
    end.

do_delete_bag_(?IS_DB = Db, StartKey) ->
    Tx = erlfdb:create_transaction(Db),
    DelFun = fun({EncKey, <<"mfdb_ref", MfdbPrefix/binary>>}, _LastKey) ->
                     ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, MfdbPrefix)),
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     EncKey;
                ({EncKey, _EncVal}, _LastKey) ->
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     EncKey
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
do_delete_bag_(?IS_TX = Tx, StartKey) ->
    %% IMPORTANT: We do *not* commit the transaction here
    %% as the caller passed in an existing transaction
    %% and they are responsible for the commit
    DelFun = fun({EncKey, <<"mfdb_ref", MfdbPrefix/binary>>}, _LastKey) ->
                     ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, MfdbPrefix)),
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     EncKey;
                ({EncKey, _EncVal}, _LastKey) ->
                     ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
                     EncKey
             end,
    try erlfdb:wait(erlfdb:fold_range(Tx, StartKey, erlfdb_key:strinc(StartKey), DelFun, [], [{limit, 100}])) of
        NextKey ->
            NextKey
    catch
        FoldErr:FoldErrMsg ->
            lager:error("Error with in deleting bag entries: ~p ~p",
                        [FoldErr, FoldErrMsg]),
            {error, FoldErrMsg}
    end.

bin_split(Bin) ->
    bin_split(Bin, 0, []).

bin_split(<<>>, _, Acc) ->
    Acc;
bin_split(<<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>, Inc, Acc) ->
    bin_split(Rest, Inc + 1, [{Inc, Part} | Acc]);
bin_split(Tail, Inc, Acc) ->
    bin_split(<<>>, Inc, [{Inc, Tail} | Acc]).

bin_join(BinList) ->
    bin_join(lists:keysort(1, BinList), <<>>).

bin_join([], Acc) ->
    Acc;
bin_join([{_, Bin} | Rest], Acc) ->
    bin_join(Rest, <<Acc/binary, Bin/binary>>).

decode_key(<<S:8, R/binary>>, TableId) ->
    <<TableId:S/bits, Bin/binary>> = R,
    case sext:decode(Bin) of
        {<<"d">>, Key} ->
            Key;
        {<<"b">>, Key, _Suffix} ->
            Key;
        BadVal ->
            exit({TableId, BadVal})
    end.

decode_val(Db, <<"mfdb_ref", Key/binary>>) ->
    Parts = erlfdb:get_range_startswith(Db, Key),
    binary_to_term(bin_join(Parts));
decode_val(_Db, CodedVal) ->
    binary_to_term(CodedVal).

encode_key(TableId, Key) ->
    <<(bit_size(TableId)):8, TableId/binary, (sext:encode(Key))/binary>>.

encode_prefix(TableId, Key) ->
    <<(bit_size(TableId)):8, TableId/binary, (sext:prefix(Key))/binary>>.

save_parts(?IS_TX = Tx, TableId, Hca, Bin) ->
    PartId = erlfdb_hca:allocate(Hca, Tx),
    PartKey = encode_prefix(TableId, {<<"p">>, PartId, <<"_">>, '_'}),
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
