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

% MW Basically a copy/paste of fabric_view_map.erl with some modifications for
% spatial queries.

-module(fabric_spatial).

-export([go/6]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch/include/couch_spatial.hrl").

go(DbName, GroupId, View, Args, Callback, Acc0) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, View, Args, Callback, Acc0);

go(DbName, DDoc, View, Args, Callback, Acc0) ->
    Shards = fabric_view:get_shards(DbName, Args),
    Workers = fabric_util:submit_jobs(Shards, spatial_index, [DDoc, View, Args]),
    BufferSize = couch_config:get("fabric", "map_buffer_size", "2"),
    State = #collector{
        db_name=DbName,
        query_args = Args,
        callback = Callback,
        buffer_size = list_to_integer(BufferSize),
        counters = fabric_dict:init(Workers, 0),
        skip = 0,               % MW skip and limit use couch_db.hrl defaults
        limit = 10000000000,
        sorted = true,          % MW don't really sort geo queries BUT weird things happen
                                % if we treat it as unsorted
        user_acc = Acc0
    },
    RexiMon = fabric_util:create_monitors(Workers),
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3,
        State, infinity, 1000 * 60 * 60) of
    {ok, NewState} ->
        {ok, NewState#collector.user_acc};
    {timeout, NewState} ->
        Callback({error, timeout}, NewState#collector.user_acc);
    {error, Resp} ->
        {ok, Resp}
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _, State) ->
    #collector{counters = Counters} = State,
    NewCounters =
        fabric_dict:filter(fun(#shard{node=Node}, _) ->
                                Node =/= NodeRef
                       end, Counters),
    {ok, State#collector{counters=NewCounters}};

handle_message({rexi_EXIT, Reason}, Worker, State) ->
    #collector{callback=Callback, counters=Counters0, user_acc=Acc} = State,
    Counters = fabric_dict:erase(Worker, Counters0),
    case fabric_view:is_progress_possible(Counters) of
    true ->
        {ok, State#collector{counters = Counters}};
    false ->
        {ok, Resp} = Callback({error, fabric_util:error_info(Reason)}, Acc),
        {error, Resp}
    end;

handle_message({total_and_updateseq, Tot, Seq}, {Worker, From}, State) ->
    #collector{
        callback = Callback,
        counters = Counters0,
        total_rows = Total0,
        offset = Offset0,
        user_acc = AccIn,
        update_seq = UpdateSeq
    } = State,
    % MW Currently sum the update seq values for each shard
    % Don't think this is the right thing to do though...
    NewSeq = Seq + UpdateSeq,
    case fabric_dict:lookup_element(Worker, Counters0) of
    undefined ->
        % this worker lost the race with other partition copies, terminate
        gen_server:reply(From, stop),
        {ok, State};
    0 ->
        gen_server:reply(From, ok),
        Counters1 = fabric_dict:update_counter(Worker, 1, Counters0),
        Counters2 = fabric_view:remove_overlapping_shards(Worker, Counters1),
        Total = Total0 + Tot,
        % MW This is probably hokey - should just remove Offset stuff for now
        Offset = Offset0,
        case fabric_dict:any(0, Counters2) of
        true ->
            {ok, State#collector{
                counters = Counters2,
                total_rows = Total,
                offset = Offset,
                update_seq = NewSeq
            }};
        false ->
            FinalOffset = erlang:min(Total, Offset+State#collector.skip),
            {Go, Acc} = Callback({update_seq, NewSeq}, AccIn),
            Counters3 = fabric_dict:decrement_all(Counters2),
            {Go, State#collector{
                counters = fabric_dict:decrement_all(Counters2),
                total_rows = Total,
                offset = FinalOffset,
                user_acc = Acc,
                update_seq = NewSeq
            }}
        end
    end;

handle_message(#view_row{} = Row, {_,From}, #collector{sorted=false} = St) ->
    #collector{callback=Callback, user_acc=AccIn, limit=Limit} = St,
    {Go, Acc} = Callback(fabric_view:transform_row(Row), AccIn),
    gen_server:reply(From, ok),
    {Go, St#collector{user_acc=Acc, limit=Limit-1}};

handle_message(#view_row{} = Row, {Worker, From}, State) ->
    #collector{
        counters = Counters0,
        rows = Rows0
    } = State,
    % MW merge only uses Id, not Key to compare rows
    Rows = merge_row(Row#view_row{worker=Worker}, Rows0),
    Counters1 = fabric_dict:update_counter(Worker, 1, Counters0),
    State1 = State#collector{rows=Rows, counters=Counters1},
    State2 = fabric_view:maybe_pause_worker(Worker, From, State1),
    fabric_view:maybe_send_row(State2);

handle_message(complete, Worker, State) ->
    Counters = fabric_dict:update_counter(Worker, 1, State#collector.counters),
    fabric_view:maybe_send_row(State#collector{counters = Counters}).

% MW Forward merge only
% Comparison function only uses Ids, not keys
merge_row(Row, Rows) ->
    lists:merge(fun(#view_row{key=KeyA, id=IdA}, #view_row{key=KeyB, id=IdB}) ->
        couch_view:less_json([IdA], [IdB])
    end, [Row], Rows).