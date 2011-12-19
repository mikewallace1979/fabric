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

-module(fabric_view_reduce).

-export([go/6]).

-include("fabric.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(DbName, GroupId, View, Args, Callback, Acc0) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, View, Args, Callback, Acc0);

go(DbName, DDoc, VName, Args, Callback, Acc0) ->
    {ok, #mrst{language=Lang, views=Views}} = couch_mrview_util:ddoc_to_mrst(
            DbName, DDoc),
    {NthRed, View} = fabric_view:extract_view(nil, VName, Views, reduce),
    {VName, RedSrc} = lists:nth(NthRed, View#mrview.reduce_funs),
    Workers = lists:map(fun(#shard{name=Name, node=N} = Shard) ->
        Ref = rexi:cast(N, {fabric_rpc, reduce_view, [Name,DDoc,VName,Args]}),
        Shard#shard{ref = Ref}
    end, fabric_view:get_shards(DbName, Args)),
    RexiMon = fabric_util:create_monitors(Workers),
    BufferSize = couch_config:get("fabric", "reduce_buffer_size", "20"),
    #mrargs{limit = Limit, skip = Skip} = Args,
    State = #collector{
        db_name = DbName,
        query_args = Args,
        callback = Callback,
        buffer_size = list_to_integer(BufferSize),
        counters = fabric_dict:init(Workers, 0),
        keys = Args#mrargs.keys,
        skip = Skip,
        limit = Limit,
        lang = Lang,
        os_proc = couch_query_servers:get_os_process(Lang),
        reducer = RedSrc,
        rows = dict:new(),
        user_acc = Acc0
    },
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
        fabric_util:cleanup(Workers),
        catch couch_query_servers:ret_os_process(State#collector.os_proc)
    end.

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _, State) ->
    fabric_view:remove_down_shards(State, NodeRef);

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

handle_message(#view_row{key=Key} = Row, {Worker, From}, State) ->
    #collector{counters = Counters0, rows = Rows0} = State,
    case fabric_dict:lookup_element(Worker, Counters0) of
    undefined ->
        % this worker lost the race with other partition copies, terminate it
        gen_server:reply(From, stop),
        {ok, State};
    _ ->
        Rows = dict:append(Key, Row#view_row{worker=Worker}, Rows0),
        C1 = fabric_dict:update_counter(Worker, 1, Counters0),
        % TODO time this call, if slow don't do it every time
        C2 = fabric_view:remove_overlapping_shards(Worker, C1),
        State1 = State#collector{rows=Rows, counters=C2},
        State2 = fabric_view:maybe_pause_worker(Worker, From, State1),
        fabric_view:maybe_send_row(State2)
    end;

handle_message(complete, Worker, State) ->
    C1 = fabric_dict:update_counter(Worker, 1, State#collector.counters),
    C2 = fabric_view:remove_overlapping_shards(Worker, C1),
    fabric_view:maybe_send_row(State#collector{counters = C2});

handle_message({meta, Meta}, {Worker, From}, State) ->
    #collector{
        callback = Callback,
        counters = Counters0,
        user_acc = AccIn
    } = State,
    case fabric_dict:lookup_element(Worker, Counters0) of
    undefined ->
        % this worker lost the race with other partition copies, terminate
        gen_server:reply(From, stop),
        {ok, State};
    0 ->
        gen_server:reply(From, ok),
        C1 = fabric_dict:update_counter(Worker, 1, Counters0),
        % TODO time this call, if slow don't do it every time
        C2 = fabric_view:remove_overlapping_shards(Worker, C1),
        State1 = State#collector{counters=C2},
        State2 = fabric_view:maybe_pause_worker(Worker, From, State1),
        case fabric_dict:any(0, C2) of
        true ->
            {ok, State2};
        false ->
            {Go, Acc} = Callback({meta, Meta}, AccIn),
            {Go, State2#collector{
                counters = fabric_dict:decrement_all(C2),
                user_acc = Acc
            }}
        end
    end.


complete_worker_test() ->
    Shards =
        mem3_util:create_partition_map("foo",3,3,[node(),node(),node()]),
    Workers = lists:map(fun(#shard{} = Shard) ->
                            Ref = make_ref(),
                            Shard#shard{ref = Ref}
                        end,
                        Shards),
    State = #collector{counters=fabric_dict:init(Workers,0)},
    {ok, NewState} = handle_message(complete, lists:nth(2,Workers), State),
    ?assertEqual(orddict:size(NewState#collector.counters),length(Workers) - 2).
