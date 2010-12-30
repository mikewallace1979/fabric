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
        {Shards, Doc}  =
                case mem3:choose_shards(DbName, Options) of
                {existing, ExistingShards} ->
                        {ExistingShards, mem3_util:open_db_doc(DbName)};
                {new, NewShards} ->
                    {NewShards, make_document(NewShards)}
                end,
        Workers = fabric_util:submit_jobs(Shards, create_db, []),
        W = couch_util:get_value(w, Options, couch_config:get("cluster","w","2")),
        Acc0 = {length(Workers), list_to_integer(W), fabric_dict:init(Workers, nil)},
        case fabric_util:recv(Workers, #shard.ref, fun handle_create_db/3, Acc0) of
            {ok, _} ->
                %% shards were created ok,
                %% now update shard_db, note that these must go across all the nodes
                %% and not just those holding shards
                case update_shard_db(Doc) of
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

update_shard_db(Doc) ->
    AllNodes = erlang:nodes([this, visible]),
    Shards = [#shard{node=N} || N <- mem3:nodes(), lists:member(N,AllNodes)],
    Workers = fabric_util:submit_jobs(Shards,create_shard_db_doc, [Doc]),
    %Majority = (length(Workers) div 2) + 1,
    Majority = length(Workers),
    Acc = {length(Workers), Majority, fabric_dict:init(Workers, nil)},
    fabric_util:recv(Workers, #shard.ref, fun handle_update_shard_db/3, Acc).


handle_create_db(Msg, Worker, Acc0) ->
    {WaitingCount, W, Counters} = Acc0,
    C1 = fabric_dict:store(Worker, Msg, Counters),
    case WaitingCount of
    1 ->
        % we're done regardless of W, finish up
        final_answer(C1);
    _ ->
        case quorum_met(W, C1) of
        true ->
            final_answer(C1);
        false ->
            {ok, {WaitingCount-1,W,C1}}
        end
    end.

quorum_met(W,C1) ->
    CompletedNodes = completed_nodes(C1, fabric_util:shard_nodes(C1)),
    length(CompletedNodes) >= W.

completed_nodes(Counters,Nodes) ->
    lists:foldl(fun(Node,Acc) ->
                        case lists:all(fun({#shard{node=NodeS},M}) ->
                                               case NodeS == Node of
                                               true ->
                                                   M =/= nil;
                                               false ->
                                                   true
                                               end
                                       end,Counters) of
                        true ->
                            [Node | Acc];
                        false ->
                            Acc
                        end
                end,[],Nodes).

handle_update_shard_db(Msg, Worker, Acc) ->
    {WaitingCount, Majority, Counters} = Acc,
    C1 = fabric_dict:store(Worker, Msg, Counters),
    case WaitingCount of
    1 ->
        {stop, ok};
    _ ->
        case completed_counters(C1) >= Majority of
        true ->
            {stop, ok};
        false ->
            {ok, {WaitingCount-1, Majority, C1}}
        end
    end.

completed_counters(Counters) ->
    length(lists:foldl(fun({_,M},Acc) ->
                               case M =/= nil of
                               true ->
                                   [M | Acc];
                               false ->
                                   Acc
                               end
                       end,[],Counters)).

make_document([#shard{name=Name, dbname=DbName}|_] = Shards) ->
    {RawOut, ByNodeOut, ByRangeOut} =
    lists:foldl(fun(#shard{node=N, range=[B,E]}, {Raw, ByNode, ByRange}) ->
        Range = ?l2b([couch_util:to_hex(<<B:32/integer>>), "-",
            couch_util:to_hex(<<E:32/integer>>)]),
        Node = couch_util:to_binary(N),
        {[[<<"add">>, Range, Node] | Raw], orddict:append(Node, Range, ByNode),
            orddict:append(Range, Node, ByRange)}
    end, {[], [], []}, Shards),
    #doc{id=DbName, body = {[
        {<<"shard_suffix">>, mem3:db_suffix(Name)},
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
    Acc0 = {36, 3, fabric_dict:init(Shards, nil)},
    Result = lists:foldl(fun(Shard,{Acc,_}) ->
                        case handle_create_db(ok,Shard,Acc) of
                        {ok, NewAcc} ->
                            {NewAcc,true};
                        {stop, ok} -> {Acc,true};
                        {error, _} -> {Acc, false}
                        end end, {Acc0, true}, Shards),
    ?assertEqual(element(2,Result), true).

db_create_file_exists_test() ->
    Shards = mem3_util:create_partition_map("foo",3,12,["node1","node2","node3","node4","node5"],"foo"),
    BadNo = random:uniform(length(Shards)),
    Acc0 = {36, 5, fabric_dict:init(Shards, nil)},
    Result = lists:foldl(
               fun(Shard,{Acc,Iter,Bool}) ->
                       MessResult = case Iter of
                                    BadNo ->
                                        handle_create_db(file_exists,Shard,Acc);
                                    _ ->
                                        handle_create_db(ok,Shard,Acc)
                                    end,
                       case MessResult of
                       {ok, NewAcc} ->
                           {NewAcc, Iter+1, Bool};
                       {stop, ok} -> {Acc, Iter+1, Bool};
                       {error, _} -> {Acc, Iter+1, false}
                       end
               end,{Acc0, 1, true}, Shards),
    ?assertEqual(element(3,Result),false).
