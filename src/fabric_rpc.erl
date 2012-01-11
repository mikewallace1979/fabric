% Copyright 2010 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_rpc).

-export([get_db_info/1, get_doc_count/1, get_update_seq/1]).
-export([open_doc/3, open_revs/4, get_missing_revs/2, get_missing_revs/3,
    update_docs/3]).
-export([all_docs/2, changes/3, map_view/4, reduce_view/4, group_info/2]).
-export([create_db/1, delete_db/1, reset_validation_funs/1, set_security/3,
    set_revs_limit/3, create_shard_db_doc/2, delete_shard_db_doc/2]).

-include("fabric.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").

%% rpc endpoints
%%  call to with_db will supply your M:F with a #db{} and then remaining args
all_docs(DbName, Args0) ->
    all_docs(DbName, Args0, fun default_cb/2, []).
all_docs(DbName, #mrargs{keys=undefined} = Args0, Callback, Acc) ->
    {ok, Db} = get_or_create_db(DbName, []),
    Sig = couch_util:with_db(Db, fun(WDb) ->
        {ok, Info} = couch_db:get_db_info(WDb),
        couch_index_util:hexsig(couch_util:md5(term_to_binary(Info)))
    end),
    Args1 = Args0#mrargs{view_type=map},
    Args2 = couch_mrview_util:validate_args(Args1),
    {ok, Acc1} = case Args2#mrargs.preflight_fun of
        PFFun when is_function(PFFun, 2) -> PFFun(Sig, Acc);
        _ -> {ok, Acc}
    end,
    all_docs_fold(Db, Args2, Callback, Acc1).

changes(DbName, Args, StartSeq) ->
    erlang:put(io_priority, {interactive, DbName}),
    #changes_args{dir=Dir} = Args,
    case get_or_create_db(DbName, []) of
    {ok, Db} ->
        Enum = fun changes_enumerator/2,
        Opts = [{dir,Dir}],
        Acc0 = {Db, StartSeq, Args},
        try
            {ok, {_, LastSeq, _}} =
                couch_db:changes_since(Db, StartSeq, Enum, Opts, Acc0),
            rexi:reply({complete, LastSeq})
        after
            couch_db:close(Db)
        end;
    Error ->
        rexi:reply(Error)
    end.

map_view(DbName, DDoc, ViewName, QueryArgs) ->
    {ok, Db} = get_or_create_db(DbName, []),
    #mrargs{
        limit = Limit,
        skip = Skip,
        extra = Extra
    } = QueryArgs,
    set_io_priority(DbName, Extra),
    QueryArgs1 = couch_mrview_util:validate_args(QueryArgs),
    {ok, {_Type, View}, _Sig, _Args} = couch_mrview_util:get_view(
            Db, DDoc, ViewName, QueryArgs1),
    {ok, Total} = couch_mrview_util:get_row_count(View),
    Acc0 = #mracc{
        db = Db,
        limit = Limit,
        skip = Skip,
        total_rows = Total,
        reduce_fun = fun couch_mrview_util:reduce_to_count/1,
        update_seq = View#mrview.update_seq,
        args = QueryArgs1
    },
    OptList = couch_mrview_util:key_opts(QueryArgs1),
    {Reds, Acc} = lists:foldl(fun(Opts, AccIn) ->
        {ok, R, A} = couch_mrview_util:fold(View, fun map_fold/3, AccIn,
            Opts),
        {R, A}
    end, Acc0, OptList),
    Offset = couch_mrview_util:reduce_to_count(Reds),
    finish_fold(Acc, [{total, Total}, {offset, Offset}]).

reduce_view(DbName, DDoc, ViewName, QueryArgs) ->
    erlang:put(io_priority, {interactive, DbName}),
    {ok, Db} = get_or_create_db(DbName, []),
    #mrargs{
        group_level = GroupLevel,
        limit = Limit,
        skip = Skip,
        extra = Extra
    } = QueryArgs,
    set_io_priority(DbName, Extra),
    GroupFun = group_rows_fun(GroupLevel),
    {ok, {_Type, {_Nth, _Lang, View}=RedView}, _Sig, _Args} =
            couch_mrview_util:get_view(Db, DDoc, ViewName, QueryArgs),
    Acc = #mracc{
        db = Db,
        total_rows = null,
        group_level = GroupLevel,
        limit = Limit,
        skip = Skip,
        update_seq = View#mrview.update_seq,
        args = QueryArgs
    },
    OptList = couch_mrview_util:key_opts(QueryArgs, [{key_group_fun, GroupFun}]),
    Acc2 = lists:foldl(fun(Opts, Acc0) ->
        {ok, Acc1} =
            couch_mrview_util:fold_reduce(RedView, fun red_fold/3, Acc0, Opts),
        Acc1
    end, Acc, OptList),
    finish_fold(Acc2, []).

create_db(DbName) ->
    rexi:reply(case couch_server:create(DbName, []) of
    {ok, _} ->
        ok;
    Error ->
        Error
    end).

create_shard_db_doc(_, Doc) ->
    rexi:reply(mem3_util:write_db_doc(Doc)).

delete_db(DbName) ->
    couch_server:delete(DbName, []).

delete_shard_db_doc(_, DocId) ->
    rexi:reply(mem3_util:delete_db_doc(DocId)).

get_db_info(DbName) ->
    with_db(DbName, [], {couch_db, get_db_info, []}).

get_doc_count(DbName) ->
    with_db(DbName, [], {couch_db, get_doc_count, []}).

get_update_seq(DbName) ->
    with_db(DbName, [], {couch_db, get_update_seq, []}).

set_security(DbName, SecObj, Options) ->
    with_db(DbName, Options, {couch_db, set_security, [SecObj]}).

set_revs_limit(DbName, Limit, Options) ->
    with_db(DbName, Options, {couch_db, set_revs_limit, [Limit]}).

open_doc(DbName, DocId, Options) ->
    with_db(DbName, Options, {couch_db, open_doc, [DocId, Options]}).

open_revs(DbName, Id, Revs, Options) ->
    with_db(DbName, Options, {couch_db, open_doc_revs, [Id, Revs, Options]}).

get_missing_revs(DbName, IdRevsList) ->
    get_missing_revs(DbName, IdRevsList, []).

get_missing_revs(DbName, IdRevsList, Options) ->
    % reimplement here so we get [] for Ids with no missing revs in response
    set_io_priority(DbName, Options),
    rexi:reply(case get_or_create_db(DbName, Options) of
    {ok, Db} ->
        Ids = [Id1 || {Id1, _Revs} <- IdRevsList],
        {ok, lists:zipwith(fun({Id, Revs}, FullDocInfoResult) ->
            case FullDocInfoResult of
            {ok, #full_doc_info{rev_tree=RevisionTree} = FullInfo} ->
                MissingRevs = couch_key_tree:find_missing(RevisionTree, Revs),
                {Id, MissingRevs, possible_ancestors(FullInfo, MissingRevs)};
            not_found ->
                {Id, Revs, []}
            end
        end, IdRevsList, couch_btree:lookup(Db#db.id_tree, Ids))};
    Error ->
        Error
    end).

update_docs(DbName, Docs0, Options) ->
    case proplists:get_value(replicated_changes, Options) of
    true ->
        X = replicated_changes;
    _ ->
        X = interactive_edit
    end,
    Docs = make_att_readers(Docs0),
    with_db(DbName, Options, {couch_db, update_docs, [Docs, Options, X]}).

group_info(DbName, DDoc) ->
    {ok, Db} = get_or_create_db(DbName, []),
    {ok, Pid} = couch_index_server:get_index(couch_mrview_index, Db, DDoc),
    rexi:reply(couch_index:get_info(Pid)).

reset_validation_funs(DbName) ->
    case get_or_create_db(DbName, []) of
    {ok, #db{main_pid = Pid}} ->
        gen_server:cast(Pid, {load_validation_funs, undefined});
    _ ->
        ok
    end.

%%
%% internal
%%

with_db(DbName, Options, {M,F,A}) ->
    set_io_priority(DbName, Options),
    case get_or_create_db(DbName, Options) of
    {ok, Db} ->
        rexi:reply(try
            apply(M, F, [Db | A])
        catch Exception ->
            Exception;
        error:Reason ->
            twig:log(error, "rpc ~p:~p/~p ~p ~p", [M, F, length(A)+1, Reason,
                clean_stack()]),
            {error, Reason}
        end);
    Error ->
        rexi:reply(Error)
    end.

get_or_create_db(DbName, Options) ->
    case couch_db:open_int(DbName, Options) of
    {not_found, no_db_file} ->
        twig:log(warn, "~p creating ~s", [?MODULE, DbName]),
        couch_server:create(DbName, Options);
    Else ->
        Else
    end.

map_fold(#full_doc_info{} = FullDocInfo, OffsetReds, Acc) ->
    % matches for _all_docs and translates #full_doc_info{} -> KV pair
    case couch_doc:to_doc_info(FullDocInfo) of
        #doc_info{id=Id, revs=[#rev_info{deleted=false, rev=Rev}|_]} = DI ->
            Value = {[{rev, couch_doc:rev_to_str(Rev)}]},
            map_fold({{Id, Id}, Value}, OffsetReds, Acc#mracc{doc_info=DI});
        #doc_info{revs=[#rev_info{deleted=true}|_]} ->
            {ok, Acc}
    end;
map_fold(_KV, _Offset, #mracc{skip=N}=Acc) when N > 0 ->
    {ok, Acc#mracc{skip=N-1, last_go=ok}};
map_fold(KV, OffsetReds, #mracc{offset=undefined}=Acc) ->
    #mracc{
        total_rows=Total,
        reduce_fun=Reduce,
        update_seq=UpdateSeq,
        args=Args
    } = Acc,
    Offset = Reduce(OffsetReds),
    Meta = make_meta(Args, UpdateSeq, [{total, Total}, {offset, Offset}]),
    Acc1 = Acc#mracc{meta_sent=true, offset=Offset},
    case rexi:sync_reply(Meta) of
    ok ->
        map_fold(KV, OffsetReds, Acc1#mracc{offset=Offset});
    stop ->
        exit(normal);
    timeout ->
        exit(timeout)
    end;
map_fold(_KV, _Offset, #mracc{limit=0}=Acc) ->
    {stop, Acc};
map_fold({{Key, Id}, Val}, _Offset, Acc) ->
    #mracc{
        db=Db,
        limit=Limit,
        doc_info=DI,
        args=Args
    } = Acc,
    Doc = case DI of
        #doc_info{} -> couch_mrview_util:maybe_load_doc(Db, DI, Args);
        _ -> couch_mrview_util:maybe_load_doc(Db, Id, Val, Args)
    end,
    Row = case Doc of
        [] -> #view_row{key=Key, id=Id, value=Val, doc=undefined};
        _ -> #view_row{key=Key, id=Id, value=Val,
                doc=couch_util:get_value(doc, Doc)}
    end,
    case rexi:sync_reply(Row) of
        ok ->
            {ok, Acc#mracc{limit=Limit-1}};
        timeout ->
            exit(timeout)
    end.


red_fold(_Key, _Red, #mracc{skip=N}=Acc) when N > 0 ->
    {ok, Acc#mracc{skip=N-1, last_go=ok}};
red_fold(Key, Red, #mracc{meta_sent=false}=Acc) ->
    #mracc{
        args=Args,
        update_seq=UpdateSeq
    } = Acc,
    Meta = make_meta(Args, UpdateSeq, []),
    case rexi:sync_reply(Meta) of
        ok ->
            Acc1 = Acc#mracc{meta_sent=true, last_go=ok},
            red_fold(Key, Red, Acc1);
        stop ->
            exit(normal);
        timeout ->
            exit(timeout)
    end;
red_fold(_Key, _Red, #mracc{limit=0} = Acc) ->
    {stop, Acc};
red_fold(_Key, Red, #mracc{group_level=0} = Acc) ->
    #mracc{
        limit=Limit
    } = Acc,
    Row = #view_row{key=null, value=Red},
    case rexi:sync_reply(Row) of
        ok ->
            {ok, Acc#mracc{limit=Limit-1}};
        timeout ->
            exit(timeout)
    end;
red_fold(Key, Red, #mracc{group_level=exact} = Acc) ->
    #mracc{
        limit=Limit
    } = Acc,
    Row = #view_row{key=Key, value=Red},
    case rexi:sync_reply(Row) of
        ok ->
            {ok, Acc#mracc{limit=Limit-1}};
        timeout ->
            exit(timeout)
    end;
red_fold(K, Red, #mracc{group_level=I} = Acc) when I > 0, is_list(K) ->
    #mracc{
        limit=Limit
    } = Acc,
    Row = #view_row{key=lists:sublist(K, I), value=Red},
    case rexi:sync_reply(Row) of
        ok ->
            {ok, Acc#mracc{limit=Limit-1}};
        timeout ->
            exit(timeout)
    end;
red_fold(K, Red, #mracc{group_level=I} = Acc) when I > 0 ->
    #mracc{
        limit=Limit
    } = Acc,
    Row = #view_row{key=K, value=Red},
    case rexi:sync_reply(Row) of
        ok ->
            {ok, Acc#mracc{limit=Limit-1}};
        timeout ->
            exit(timeout)
    end.


all_docs_fold(Db, #mrargs{keys=undefined}=Args, Callback, UAcc) ->
    {ok, Info} = couch_db:get_db_info(Db),
    Total = couch_util:get_value(doc_count, Info),
    UpdateSeq = couch_db:get_update_seq(Db),
    Acc = #mracc{
        db=Db,
        total_rows=Total,
        limit=Args#mrargs.limit,
        skip=Args#mrargs.skip,
        callback=Callback,
        user_acc=UAcc,
        reduce_fun=fun couch_mrview_util:all_docs_reduce_to_count/1,
        update_seq=UpdateSeq,
        args=Args
    },
    [Opts] = couch_mrview_util:all_docs_key_opts(Args),
    {ok, Offset, FinalAcc} = couch_db:enum_docs(Db, fun map_fold/3, Acc, Opts),
    finish_fold(FinalAcc, [{total, Total}, {offset, Offset}]);
all_docs_fold(Db, #mrargs{direction=Dir, keys=Keys0}=Args, Callback, UAcc) ->
    {ok, Info} = couch_db:get_db_info(Db),
    Total = couch_util:get_value(doc_count, Info),
    UpdateSeq = couch_db:get_update_seq(Db),
    Acc = #mracc{
        db=Db,
        total_rows=Total,
        limit=Args#mrargs.limit,
        skip=Args#mrargs.skip,
        callback=Callback,
        user_acc=UAcc,
        reduce_fun=fun couch_mrview_util:all_docs_reduce_to_count/1,
        update_seq=UpdateSeq,
        args=Args
    },
    % Backwards compatibility hack. The old _all_docs iterates keys
    % in reverse if descending=true was passed. Here we'll just
    % reverse the list instead.
    Keys = if Dir =:= fwd -> Keys0; true -> lists:reverse(Keys0) end,

    FoldFun = fun(Key, Acc0) ->
        DocInfo = (catch couch_db:get_doc_info(Db, Key)),
        {Doc, Acc1} = case DocInfo of
            {ok, #doc_info{id=Id, revs=[RevInfo | _RestRevs]}=DI} ->
                Rev = couch_doc:rev_to_str(RevInfo#rev_info.rev),
                Props = [{rev, Rev}] ++ case RevInfo#rev_info.deleted of
                    true -> [{deleted, true}];
                    false -> []
                end,
                {{{Id, Id}, {Props}}, Acc0#mracc{doc_info=DI}};
            not_found ->
                {{{Key, error}, not_found}, Acc0}
        end,
        {_, Acc2} = map_fold(Doc, {[], [{0, 0, 0}]}, Acc1),
        Acc2
    end,
    FinalAcc = lists:foldl(FoldFun, Acc, Keys),
    finish_fold(FinalAcc, [{total, Total}]).

finish_fold(#mracc{last_go=ok, update_seq=UpdateSeq}=Acc,  ExtraMeta) ->
    #mracc{args=Args}=Acc,
    % Possible send meta info
    Meta = make_meta(Args, UpdateSeq, ExtraMeta),
    case Acc#mracc.meta_sent of
        false ->
            case rexi:sync_reply(Meta) of
                ok ->
                    rexi:reply(complete);
                stop ->
                    {ok, Acc#mracc.user_acc};
                timeout ->
                    exit(timeout)
            end;
        _ -> rexi:reply(complete)
    end;
finish_fold(#mracc{user_acc=_UAcc}, _ExtraMeta) ->
    rexi:reply(complete).


%% TODO: handle case of bogus group level
group_rows_fun(exact) ->
    fun({Key1,_}, {Key2,_}) -> Key1 == Key2 end;
group_rows_fun(0) ->
    fun(_A, _B) -> true end;
group_rows_fun(GroupLevel) when is_integer(GroupLevel) ->
    fun({[_|_] = Key1,_}, {[_|_] = Key2,_}) ->
        lists:sublist(Key1, GroupLevel) == lists:sublist(Key2, GroupLevel);
    ({Key1,_}, {Key2,_}) ->
        Key1 == Key2
    end.


changes_enumerator(DocInfo, {Db, _Seq, Args}) ->
    #changes_args{
        include_docs = IncludeDocs,
        filter = Acc,
        conflicts = Conflicts
    } = Args,
    #doc_info{high_seq=Seq, revs=[#rev_info{deleted=Del}|_]} = DocInfo,
    case [X || X <- couch_changes:filter(DocInfo, Acc), X /= null] of
    [] ->
        {ok, {Db, Seq, Args}};
    Results ->
        Opts = if Conflicts -> [conflicts]; true -> [] end,
        ChangesRow = changes_row(Db, DocInfo, Results, Del, IncludeDocs, Opts),
        Go = rexi:sync_reply(ChangesRow),
        {Go, {Db, Seq, Args}}
    end.

changes_row(Db, #doc_info{id=Id, high_seq=Seq}=DI, Results, Del, true, Opts) ->
    Doc = doc_member(Db, DI, Opts),
    #change{key=Seq, id=Id, value=Results, doc=Doc, deleted=Del};
changes_row(_, #doc_info{id=Id, high_seq=Seq}, Results, true, _, _) ->
    #change{key=Seq, id=Id, value=Results, deleted=true};
changes_row(_, #doc_info{id=Id, high_seq=Seq}, Results, _, _, _) ->
    #change{key=Seq, id=Id, value=Results}.

doc_member(Shard, DocInfo, Opts) ->
    case couch_db:open_doc(Shard, DocInfo, [deleted | Opts]) of
    {ok, Doc} ->
        couch_doc:to_json_obj(Doc, []);
    Error ->
        Error
    end.

possible_ancestors(_FullInfo, []) ->
    [];
possible_ancestors(FullInfo, MissingRevs) ->
    #doc_info{revs=RevsInfo} = couch_doc:to_doc_info(FullInfo),
    LeafRevs = [Rev || #rev_info{rev=Rev} <- RevsInfo],
    % Find the revs that are possible parents of this rev
    lists:foldl(fun({LeafPos, LeafRevId}, Acc) ->
        % this leaf is a "possible ancenstor" of the missing
        % revs if this LeafPos lessthan any of the missing revs
        case lists:any(fun({MissingPos, _}) ->
                LeafPos < MissingPos end, MissingRevs) of
        true ->
            [{LeafPos, LeafRevId} | Acc];
        false ->
            Acc
        end
    end, [], LeafRevs).

make_att_readers([]) ->
    [];
make_att_readers([#doc{atts=Atts0} = Doc | Rest]) ->
    % % go through the attachments looking for 'follows' in the data,
    % % replace with function that reads the data from MIME stream.
    Atts = [Att#att{data=make_att_reader(D)} || #att{data=D} = Att <- Atts0],
    [Doc#doc{atts = Atts} | make_att_readers(Rest)].

make_att_reader({follows, Parser}) ->
    fun() ->
        Parser ! {get_bytes, self()},
        receive {bytes, Bytes} -> Bytes end
    end;
make_att_reader(Else) ->
    Else.

clean_stack() ->
    lists:map(fun({M,F,A}) when is_list(A) -> {M,F,length(A)}; (X) -> X end,
        erlang:get_stacktrace()).

set_io_priority(DbName, Options) ->
    case lists:keyfind(io_priority, 1, Options) of
    {io_priority, Pri} ->
        erlang:put(io_priority, Pri);
    false ->
        erlang:put(io_priority, {interactive, DbName})
    end.


default_cb(complete, Acc) ->
    {ok, lists:reverse(Acc)};
default_cb({final, Info}, []) ->
    {ok, [Info]};
default_cb({final, _}, Acc) ->
    {ok, Acc};
default_cb(Row, Acc) ->
    {ok, [Row | Acc]}.

make_meta(Args, UpdateSeq, Base) ->
    case Args#mrargs.update_seq of
        true -> {meta, Base ++ [{update_seq, UpdateSeq}]};
        _ -> {meta, Base}
    end.
