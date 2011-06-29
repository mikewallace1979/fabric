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

-module(fabric_spatial).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_spatial.hrl").

-export([go/5]).

go(Db, DDoc, Name, Stale, DbName) ->
    ?LOG_DEBUG("Starting...", []),
    Shards = mem3:shards(DbName),
    Workers = fabric_util:submit_jobs(Shards, get_spatial_index, [Db, DDoc, Name, Stale]),
    Acc0 = {fabric_dict:init(Workers, nil), []},
    fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0).

handle_message({ok, Index, Group}, #shard{dbname=_Name} = Shard, {Counters, Acc}) ->
    ?LOG_DEBUG("Message fd ~p ~p", [?MODULE, Group#spatial_group.fd]),
    % MW: We follow the pattern used in fabric_db_info for now
    case fabric_dict:lookup_element(Shard, Counters) of
    undefined ->
        % already heard from someone else in this range
        {ok, {Counters, Acc}};
    nil ->
        C1 = fabric_dict:store(Shard, {Index, Group}, Counters),
        case fabric_dict:any(nil, C1) of
        true ->
            {ok, {C1, [{Index, Group}|Acc]}};
        false ->
            {stop, [{Index, Group}|Acc]}
        end
    end;
    %{stop, {Index, Group}};
handle_message(Foo, #shard{dbname=_Name} = _Shard, _Acc) ->
    ?LOG_INFO("Message ~p ~p", [?MODULE, Foo]),
    {ok, Foo}.