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

-module(fabric_db_create).
-export([go/2]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(DBNAME_REGEX, "^[a-z][a-z0-9\\_\\$()\\+\\-\\/\\s.]*$").

%% @doc Create a new database, and all its partition files across the cluster
%%      Options is proplist with user_ctx, n, q
go(DbName, Options) ->
    case re:run(DbName, ?DBNAME_REGEX, [{capture,none}]) of
    match ->
        {MegaSecs,Secs,_} = now(),
        Suffix = "." ++ integer_to_list(MegaSecs*1000000 + Secs),
        {Shards, Doc}  =
                case mem3:choose_shards(DbName, [{shard_suffix, Suffix}] ++ Options) of
                    {existing, ExistingShards} ->
                        {ExistingShards, mem3_util:open_db_doc(DbName)};
                    {new, NewShards} ->
                        {NewShards, make_document(NewShards, Suffix)}
                end,
        Workers = fabric_util:submit_jobs(Shards, create_db, []),
        Acc0 = fabric_dict:init(Workers, nil),
        case fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0) of
            {ok, _} ->
                %% create the shard_db docs, note that these must go across all the nodes
                %% and not just those holding shards
                Workers1 = fabric_util:submit_job_nodes(mem3:nodes(),
                                                        create_shard_db_doc, [Doc]),
                Acc1 = fabric_dict:init(Workers1, nil),
                case fabric_util:recv(Workers1, 1, fun handle_message/3, Acc1)
                of
                    {ok, _} ->
                        ok;
                    Else ->
                        Else
                end;
            Else ->
                Else
        end;
    nomatch ->
            {error, illegal_database_name}
    end.

handle_message(Msg, Worker, Counters) ->
    C1 = fabric_dict:store(Worker, Msg, Counters),
    case fabric_dict:any(nil, C1) of
    true ->
        {ok, C1};
    false ->
        % worker could be shard or a node
        case Worker of
        #shard{} ->
                final_answer(C1);
        _Any ->
                {stop, ok}
        end
    end.

make_document([#shard{dbname=DbName}|_] = Shards, Suffix) ->
    {RawOut, ByNodeOut, ByRangeOut} =
    lists:foldl(fun(#shard{node=N, range=[B,E]}, {Raw, ByNode, ByRange}) ->
        Range = ?l2b([couch_util:to_hex(<<B:32/integer>>), "-",
            couch_util:to_hex(<<E:32/integer>>)]),
        Node = couch_util:to_binary(N),
        {[[<<"add">>, Range, Node] | Raw], orddict:append(Node, Range, ByNode),
            orddict:append(Range, Node, ByRange)}
    end, {[], [], []}, Shards),
    #doc{id=DbName, body = {[
        {<<"shard_suffix">>, Suffix},
        {<<"changelog">>, lists:sort(RawOut)},
        {<<"by_node">>, {[{K,lists:sort(V)} || {K,V} <- ByNodeOut]}},
        {<<"by_range">>, {[{K,lists:sort(V)} || {K,V} <- ByRangeOut]}}
    ]}}.

final_answer(Counters) ->
    Successes = [X || {_, M} = X <- Counters, M == ok orelse M == file_exists],
    case fabric_view:is_progress_possible(Successes) of
    true ->
        case lists:keymember(file_exists, 2, Successes) of
        true ->
            {error, file_exists};
        false ->
            {stop, ok}
        end;
    false ->
        {error, internal_server_error}
    end.

db_create_ok_test() ->
    Shards = mem3_util:create_partition_map("foo",3,12,["node1","node2","node3"],"foo"),
    Acc0 = fabric_dict:init(Shards, nil),
    Result = lists:foldl(fun(Shard,{Acc,_}) ->
                        case handle_message(ok,Shard,Acc) of
                            {ok, NewAcc} ->
                                {NewAcc,true};
                            {stop, ok} -> {Acc,true};
                            {error, _} -> {Acc, false}
                        end end, {Acc0, true}, Shards),
    ?assertEqual(element(2,Result), true).

db_create_file_exists_test() ->
    Shards = mem3_util:create_partition_map("foo",3,12,["node1","node2","node3","node4","node5"],"foo"),
    BadNo = random:uniform(length(Shards)),
    Acc0 = fabric_dict:init(Shards, nil),
    Result = lists:foldl(
               fun(Shard,{Acc,Iter,Bool}) ->
                       MessResult = case Iter of
                                        BadNo ->
                                            handle_message(file_exists,Shard,Acc);
                                        _ ->
                                            handle_message(ok,Shard,Acc)
                                    end,
                       case MessResult of
                           {ok, NewAcc} ->
                               {NewAcc, Iter+1, Bool};
                           {stop, ok} -> {Acc, Iter+1, Bool};
                           {error, _} -> {Acc, Iter+1, false}
                       end
               end,{Acc0, 1, true}, Shards),
    ?assertEqual(element(3,Result),false).
