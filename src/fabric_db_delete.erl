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

-module(fabric_db_delete).
-export([go/2]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").

%% @doc Options aren't used at all now in couch on delete but are left here
%%      to be consistent with fabric_db_create for possible future use
%% @see couch_server:delete_db
%%
go(DbName, Options) ->
    DbDoc = mem3_util:open_db_doc(DbName),
    % delete shard_db DbDoc first before deleting shards
    case update_shard_db(DbDoc) of
    {ok, _} ->
        Shards = mem3:shards(DbName),
        Workers = fabric_util:submit_jobs(Shards, delete_db, []),
        W = couch_util:get_value(w, Options, couch_config:get("cluster","w","2")),
        Acc0 = {length(Workers), list_to_integer(W), fabric_dict:init(Workers, nil)},
        case fabric_util:recv(Workers, #shard.ref, fun handle_delete_db/3, Acc0) of
        {ok, ok} ->
            ok;
        {ok, not_found} ->
            erlang:error(database_does_not_exist);
        Error ->
            Error
        end;
    Else ->
        Else
    end.

update_shard_db(Doc) ->
    AllNodes = erlang:nodes([this, visible]),
    Shards = [#shard{node=N} || N <- mem3:nodes(), lists:member(N,AllNodes)],
    Workers = fabric_util:submit_jobs(Shards,delete_shard_db_doc, [Doc]),
    % Majority = (length(Workers) div 2) + 1,
    Majority = length(Workers),
    Acc = {length(Workers), Majority, fabric_dict:init(Workers, nil)},
    fabric_util:recv(Workers, #shard.ref, fun handle_update_shard_db/3, Acc).

handle_delete_db(Msg, Worker, Acc0) ->
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

final_answer(Counters) ->
    Successes = [X || {_, M} = X <- Counters, M == ok orelse M == not_found],
    case fabric_view:is_progress_possible(Successes) of
    true ->
        case lists:keymember(ok, 2, Successes) of
        true ->
            {stop, ok};
        false ->
            {stop, not_found}
        end;
    false ->
        {error, internal_server_error}
    end.
