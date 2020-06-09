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
    EncKey = sext:encode({TableId, <<"d">>, K}),
    V = term_to_binary(V0),
    %% io:format("Put:~nTableId: ~p~nK: ~p~nEncK: ~p~n V0: ~p~nV: ~p~n",
    %%           [TableId, K, EncKey, V0, V]),
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
    %% io:format("put_bag_ complete~n"),
    ok;
put_bag_(Tx, TableId, HcaBag, HcaRef, [{K, V0} | Rest]) ->
    StartKey = sext:encode({TableId, <<"b">>, K}),
    %% io:format("put_bag_ start key: sext:encode({TableId, <<\"b\">>, ~p}) -> ~p~n", [TableId, K, StartKey]),
    V = term_to_binary(V0),
    case erlfdb:wait(erlfdb:get_range(Tx, StartKey, erlfdb_key:strinc(StartKey))) of
        [] ->
            %% io:format("not matches:~n", []),
            EncKey = sext:encode({TableId, <<"b">>, K, erlfdb_hca:allocate(HcaBag, Tx)}),
            case byte_size(V) > ?MAX_VALUE_SIZE of
                true ->
                    %% io:format("We have a big bin: ~p ~p~n", [byte_size(V), ?MAX_VALUE_SIZE]),
                    MfdbRefPrefix = save_parts(Tx, TableId, HcaRef, V),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", MfdbRefPrefix/binary>>));
                false ->
                    %% io:format("We have a small bin: ~p ~p ~p~n", [Tx, EncKey, V]),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, V))
            end;
        PossMatches ->
            %% io:format("found matches: ~n~p~n", [PossMatches]),
            ok = bag_maybe_replace_(PossMatches, Tx, TableId, HcaBag, HcaRef, {K, V})
    end,
    put_bag_(Tx, TableId, HcaBag, HcaRef, Rest).

bag_maybe_replace_([], Tx, TableId, HcaBag, HcaRef, {K, V}) ->
    %% We did not have a matching value, so add
    EncKey = sext:encode({TableId, <<"b">>, K, erlfdb_hca:allocate(HcaBag, Tx)}),
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
    EncKey = sext:encode({TableId, <<"d">>, K}),
    V = term_to_binary(V0),
    %% io:format("Put:~nTableId: ~p~nK: ~p~nEncK: ~p~n V0: ~p~nV: ~p~n",
    %%           [TableId, K, EncKey, V0, V]),
    case byte_size(V) > ?MAX_VALUE_SIZE of
        true ->
            %% Ensure we remove any old parts
            Tx = erlfdb:create_transaction(Db),
            case erlfdb:get(Tx, EncKey) of
                <<"mfdb_ref", OldMfdbRefPrefix/binary>> ->
                    erlfdb:clear_range_startswith(Tx, OldMfdbRefPrefix);
                _ ->
                    ok
            end,
            %% Save the new parts
            MfdbRefPrefix = save_parts(Tx, TableId, HcaRef, V),
            ok = erlfdb:set(Tx, EncKey, <<"mfdb_ref", MfdbRefPrefix/binary>>),
            erlfdb:commit(Tx);
        false ->
            erlfdb:set(Db, EncKey, V)
    end.

delete(#st{db = DbOrTx, table_id = TableId, type = bag}, K) ->
    StartKey = sext:encode({TableId, <<"b">>, K}),
    %% When db is a transaction we do _NOT_ commit, it's the caller's responsibility
    do_delete_bag(DbOrTx, StartKey);
delete(#st{db = ?IS_DB = Db, table_id = TableId}, K) ->
    EncKey = sext:encode({TableId, <<"d">>, K}),
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
    EncKey = sext:encode({TableId, <<"d">>, K}),
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
    << <<B/bits>> || {_, <<B>>} <- lists:keysort(1, BinList) >>.

decode_key(Bin, TableId) ->
    case sext:decode(Bin) of
        {TableId, <<"d">>, Key} ->
            Key;
        {TableId, <<"b">>, Key, _Suffix} ->
            Key;
        BadVal ->
            exit({TableId, BadVal})
    end.

save_parts(?IS_TX = Tx, TableId, Hca, Bin) ->
    PartId = erlfdb_hca:allocate(Hca, Tx),
    PartKey = sext:prefix({TableId, <<"p">>, PartId, <<"_">>, '_'}),
    ok = save_parts_(Tx, TableId, PartId, 0, Bin),
    PartKey.

save_parts_(_Tx, _TableId, _PartId, _PartInc, <<>>) ->
    ok;
save_parts_(Tx, TableId, PartId, PartInc, <<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>) ->
    Key = sext:encode({TableId, <<"p">>, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, term_to_binary(PartInc, Part))),
    save_parts_(Tx, TableId, PartId, PartInc + 1, Rest);
save_parts_(Tx, TableId, PartId, PartInc, Tail) ->
    Key = sext:encode({TableId, <<"p">>, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, term_to_binary(PartInc, Tail))),
    save_parts_(Tx, TableId, PartId, PartInc + 1, <<>>).
