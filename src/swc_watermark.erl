%%%-------------------------------------------------------------------
%%% @author Ricardo Gonçalves <tome.wave@gmail.com>
%%% @copyright (C) 2016, Ricardo Gonçalves
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(swc_watermark).

-author('Ricardo Gonçalves <tome.wave@gmail.com>').

-compile({no_auto_import,[min/2]}).

-include_lib("swc/include/swc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API exports
-export([ new/0
        , add_peer/3
        , left_join/2
        , replace_peer/3
        , retire_peer/4
        , update_peer/3
        , update_cell/4
        , min/2
        , peers/1
        , get/3
        , reset_counters/1
        , delete_peer/2
        , prune_retired_peers/2
        ]).

-spec new() -> vv_matrix().
new() ->
    {orddict:new(), orddict:new()}.


-spec add_peer(vv_matrix(), id(), [id()]) -> vv_matrix().
add_peer({M,R}, NewPeer, ItsPeers) ->
    % CurrentPeers = orddict:fetch_keys(M),
    NewEntry = lists:foldl(
                 fun (Id, Acc) -> swc_vv:add(Acc, {Id,0}) end,
                 swc_vv:new(),
                 [NewPeer | ItsPeers]),
    {orddict:store(NewPeer, NewEntry, M), R}.


-spec update_peer(vv_matrix(), id(), bvv()) -> vv_matrix().
update_peer({M,R}, EntryId, NodeClock) ->
    {update_peer_aux(M, EntryId, NodeClock),
     update_peer_aux(R, EntryId, NodeClock)}.

update_peer_aux(M, EntryId, NodeClock) ->
    orddict:map(fun (Id, OldVV) ->
                    case swc_vv:is_key(OldVV, EntryId) of
                        false -> OldVV;
                        true  ->
                            {Base,_} = swc_node:get(Id, NodeClock),
                            swc_vv:add(OldVV, {EntryId, Base})
                    end
                end,
                M).

-spec replace_peer(vv_matrix(), Old::id(), New::id()) -> vv_matrix().
replace_peer({M,R}, Old, New) ->
    M3 = case orddict:is_key(Old, M) of
        true ->
            OldPeers0 = swc_vv:ids(orddict:fetch(Old,M)),
            OldPeers = lists:delete(Old, OldPeers0),
            {M2,R} = add_peer({M,R}, New, OldPeers),
            orddict:erase(Old, M2);
        false -> M
    end,
    {orddict:map(fun(_K,V) ->
                    case orddict:find(Old, V) of
                        error -> V;
                        {ok, _} ->
                            V2 = swc_vv:delete_key(V, Old),
                            swc_vv:add(V2, {New, 0})
                    end
                end, M3), R}.

-spec retire_peer(vv_matrix(), Old::id(), New::id(), Jump::non_neg_integer()) -> vv_matrix().
retire_peer({M,R}, Old, New, Jump) ->
    case orddict:find(Old, M) of
        error ->
            replace_peer({M,R}, Old, New);
        {ok, OldEntry} ->
            CurrentCounter = swc_vv:get(Old, OldEntry),
            OldEntry2 = swc_vv:add(OldEntry, {Old, CurrentCounter+Jump}),
            R1 = orddict:store(Old, OldEntry2, R),
            replace_peer({M,R1}, Old, New)
    end.


-spec left_join(vv_matrix(), vv_matrix()) -> vv_matrix().
left_join({MA,RA},{MB,RB}) ->
    {left_join_aux(MA,MB), left_join_aux(RA,RB)}.

left_join_aux(A,B) ->
    % filter entry peers from B that are not in A
    PeersA = orddict:fetch_keys(A),
    FunFilter = fun (Id,_) -> lists:member(Id, PeersA) end,
    B2 = orddict:filter(FunFilter, B),
    orddict:merge(fun (_,V1,V2) -> swc_vv:left_join(V1,V2) end, A, B2).

-spec update_cell(vv_matrix(), id(), id(), counter()) -> vv_matrix().
update_cell({M,R}, EntryId, PeerId, Counter) ->
    Top = {PeerId, Counter},
    {orddict:update(
        EntryId,
        fun (OldVV) -> swc_vv:add(OldVV, Top) end,
        swc_vv:add(swc_vv:new(), Top),
        M), R}.

-spec min(vv_matrix(), id()) -> counter().
min({M,R}, Id) ->
    max(min_aux(M, Id), min_aux(R, Id)).

min_aux(M, Id) ->
    case orddict:find(Id, M) of
        error -> 0;
        {ok, VV} -> swc_vv:min(VV)
    end.

-spec peers(vv_matrix()) -> [id()].
peers({M,_}) ->
    orddict:fetch_keys(M).

-spec get(vv_matrix(), id(), id()) -> counter().
get({M,_}, P1, P2) ->
    case orddict:find(P1, M) of
        error -> 0;
        {ok, VV} -> swc_vv:get(P2, VV)
    end.

-spec reset_counters(vv_matrix()) -> vv_matrix().
reset_counters({M,_}) ->
    {orddict:map(fun (_Id,VV) -> swc_vv:reset_counters(VV) end, M), orddict:new()}.

-spec delete_peer(vv_matrix(), id()) -> vv_matrix().
delete_peer({M,R}, Id) ->
    M2 = orddict:erase(Id, M),
    {orddict:map(fun (_Id,VV) -> swc_vv:delete_key(VV, Id) end, M2), R}.

-spec prune_retired_peers(vv_matrix(), key_matrix()) -> vv_matrix().
prune_retired_peers({M,R}, DKM) ->
    {M, orddict:filter(fun (Peer,_) -> swc_dotkeymap:is_key(DKM, Peer) end, R)}.

%%===================================================================
%% EUnit tests
%%===================================================================

-ifdef(TEST).

update_test() ->
    C1 = [{"a",{12,0}}, {"b",{7,0}}, {"c",{4,0}}, {"d",{5,0}}, {"e",{5,0}}, {"f",{7,10}}, {"g",{5,10}}, {"h",{5,14}}],
    C2 = [{"a",{5,14}}, {"b",{5,14}}, {"c",{50,14}}, {"d",{5,14}}, {"e",{15,0}}, {"f",{5,14}}, {"g",{7,10}}, {"h",{7,10}}],
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    M5 = update_cell(M4, "c", "c",15),
    M6 = update_peer(M5, "c", C1),
    M7 = update_peer(M5, "c", C2),
    M8 = update_peer(M5, "a", C1),
    M9 = update_peer(M5, "a", C2),
    M10 = update_peer(M5, "b", C1),
    M11 = update_peer(M5, "b", C2),
    N = {[{"c",[{"c",4},{"d",3},{"z",0}]}, {"d",[{"c",0},{"d",1},{"e",2}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], [{"b",[{"a",2},{"b",2},{"c",3}]}]},
    ?assertEqual( update_peer(N,"a",C1), {[{"c",[{"c",4},{"d",3},{"z",0}]}, {"d",[{"c",0},{"d",1},{"e",2}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], [{"b",[{"a",7},{"b",2},{"c",3}]}]}),
    ?assertEqual( update_peer(N,"c",C2), {[{"c",[{"c",50},{"d",3},{"z",0}]}, {"d",[{"c",5},{"d",1},{"e",2}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], [{"b",[{"a",2},{"b",2},{"c",5}]}]}),
    ?assertEqual( M1,  {[{"a",[{"b",4}]}], []}),
    ?assertEqual( M2,  {[{"a",[{"b",4}, {"c",10}]}], []}),
    ?assertEqual( M3,  {[{"a",[{"b",4}, {"c",10}]},    {"c",[{"c",2}]}], []}),
    ?assertEqual( M4,  {[{"a",[{"b",4}, {"c",10}]},    {"c",[{"c",20}]}], []}),
    ?assertEqual( M4,  M5),
    ?assertEqual( M6,  {[{"a",[{"b",4},  {"c",12}]},   {"c",[{"c",20}]}], []}),
    ?assertEqual( M7,  {[{"a",[{"b",4},  {"c",10}]},   {"c",[{"c",50}]}], []}),
    ?assertEqual( M8,  {[{"a",[{"b",4},  {"c",10}]},   {"c",[{"c",20}]}], []}),
    ?assertEqual( M9,  {[{"a",[{"b",4},  {"c",10}]},   {"c",[{"c",20}]}], []}),
    ?assertEqual( M10, {[{"a",[{"b",12}, {"c",10}]},   {"c",[{"c",20}]}], []}),
    ?assertEqual( M11, {[{"a",[{"b",5},  {"c",10}]},   {"c",[{"c",20}]}], []}).

left_join_test() ->
    A = {[{"a",[{"b",4}, {"c",10}]}, {"c",[{"c",20}]}, {"z",[{"t1",0},{"t2",0},{"z",0}]}], []},
    Z = {[{"a",[{"b",5}, {"c",8}, {"z",2}]}, {"c",[{"c",20}]}, {"z",[{"t1",0},{"t2",0},{"z",0}]}], []},
    B = {[{"a",[{"b",2}, {"c",10}]}, {"b",[]}, {"c",[{"c",22}]}], []},
    C = {[{"z",[{"a",1}, {"b",0}, {"z",4}]}], []},
    ?assertEqual( left_join(A,B), {[{"a",[{"b",4},{"c",10}]}, {"c",[{"c",22}]}, {"z",[{"t1",0},{"t2",0},{"z",0}]}], []}),
    ?assertEqual( left_join(A,Z), {[{"a",[{"b",5},{"c",10}]}, {"c",[{"c",20}]}, {"z",[{"t1",0},{"t2",0},{"z",0}]}], []}),
    ?assertEqual( left_join(A,C), {[{"a",[{"b",4},{"c",10}]}, {"c",[{"c",20}]}, {"z",[{"t1",0},{"t2",0},{"z",4}]}], []}),
    ?assertEqual( left_join(B,A), {[{"a",[{"b",4},{"c",10}]}, {"b",[]}, {"c",[{"c",22}]}], []}),
    ?assertEqual( left_join(B,C), B),
    ?assertEqual( left_join(C,A), C),
    ?assertEqual( left_join(C,B), C).


add_peers_test() ->
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    ?assertEqual( add_peer(add_peer(M, "z", ["b","a"]), "l", ["z","y"]),
                  add_peer(add_peer(M, "l", ["y","z"]), "z", ["a","b"])),
    ?assertEqual( add_peer(M, "z",["a","b"]),    {[{"z",[{"a",0},{"b",0},{"z",0}]}], []}),
    ?assertEqual( add_peer(M4, "z",["t2","t1"]), {[{"a",[{"b",4}, {"c",10}]}, {"c",[{"c",20}]}, {"z",[{"t1",0},{"t2",0},{"z",0}]}], []}).

min_test() ->
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    ?assertEqual( min(M, "a"), 0),
    ?assertEqual( min(M1, "a"), 4),
    ?assertEqual( min(M1, "b"), 0),
    ?assertEqual( min(M4, "a"), 4),
    ?assertEqual( min(M4, "c"), 20),
    ?assertEqual( min(M4, "b"), 0).

peers_test() ->
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    M5 = update_cell(M4, "c", "c",15),
    ?assertEqual( peers(M), []),
    ?assertEqual( peers(M1), ["a"]),
    ?assertEqual( peers(M5), ["a", "c"]).


get_test() ->
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    ?assertEqual( get(M, "a", "a"), 0),
    ?assertEqual( get(M1, "a", "a"), 0),
    ?assertEqual( get(M1, "b", "a"), 0),
    ?assertEqual( get(M4, "c", "c"), 20),
    ?assertEqual( get(M4, "a", "c"), 10).

reset_counters_test() ->
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    ?assertEqual( reset_counters(M), M),
    ?assertEqual( reset_counters(M1), {[{"a",[{"b",0}]}], []}),
    ?assertEqual( reset_counters(M2), {[{"a",[{"b",0}, {"c",0}]}], []}),
    ?assertEqual( reset_counters(M3), {[{"a",[{"b",0}, {"c",0}]}, {"c",[{"c",0}]}], []}),
    ?assertEqual( reset_counters(M4), {[{"a",[{"b",0}, {"c",0}]}, {"c",[{"c",0}]}], []}).

delete_peer_test() ->
    M = new(),
    M1 = update_cell(M, "a", "b",4),
    M2 = update_cell(M1, "a", "c",10),
    M3 = update_cell(M2, "c", "c",2),
    M4 = update_cell(M3, "c", "c",20),
    ?assertEqual( delete_peer(M1, "a"), {[], []}),
    ?assertEqual( delete_peer(M1, "b"), {[{"a",[]}], []}),
    ?assertEqual( delete_peer(M1, "c"), {[{"a",[{"b",4}]}], []}),
    ?assertEqual( delete_peer(M4, "a"), {[{"c",[{"c",20}]}], []}),
    ?assertEqual( delete_peer(M4, "c"), {[{"a",[{"b",4}]}], []}).

replace_peer_test() ->
    A = add_peer(new(), "a", ["b","c"]),
    B = add_peer(A,     "b", ["a","c"]),
    C = add_peer(B,     "c", ["a","b"]),
    Z = {[{"a",[{"a",9},{"c",2},{"z",3}]}, {"c",[{"a",1},{"c",4},{"z",3}]}, {"z", [{"a",0},{"c",1},{"z",2}]}], []},
    W = {[{"b",[{"a",9},{"b",2},{"c",3}]}, {"c",[{"b",1},{"c",4},{"d",3}]}, {"d", [{"c",0},{"d",1},{"e",2}]}], []},
    ?assertEqual( replace_peer(C,"b","z"), {[{"a",[{"a",0},{"c",0},{"z",0}]}, {"c",[{"a",0},{"c",0},{"z",0}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], []}),
    ?assertEqual( replace_peer(Z,"a","b"), {[{"b",[{"b",0},{"c",0},{"z",0}]}, {"c",[{"b",0},{"c",4},{"z",3}]}, {"z", [{"b",0},{"c",1},{"z",2}]}], []}),
    ?assertEqual( replace_peer(W,"b","z"), {[{"c",[{"c",4},{"d",3},{"z",0}]}, {"d",[{"c",0},{"d",1},{"e",2}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], []}),
    ?assertEqual( replace_peer(W,"a","z"), {[{"b",[{"b",2},{"c",3},{"z",0}]}, {"c",[{"b",1},{"c",4},{"d",3}]}, {"d", [{"c",0},{"d",1},{"e",2}]}], []}).

retire_peer_test() ->
    A = add_peer(new(), "a", ["b","c"]),
    B = add_peer(A,     "b", ["a","c"]),
    C = add_peer(B,     "c", ["a","b"]),
    Z = {[{"a",[{"a",9},{"c",2},{"z",3}]}, {"c",[{"a",1},{"c",4},{"z",3}]}, {"z", [{"a",0},{"c",1},{"z",2}]}], []},
    W = {[{"b",[{"a",9},{"b",2},{"c",3}]}, {"c",[{"b",1},{"c",4},{"d",3}]}, {"d", [{"c",0},{"d",1},{"e",2}]}], []},
    ?assertEqual( retire_peer(C,"b","z",100),
                  {[{"a",[{"a",0},{"c",0},{"z",0}]}, {"c",[{"a",0},{"c",0},{"z",0}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], [{"b",[{"a",0},{"b",100},{"c",0}]}]}),
    ?assertEqual( retire_peer(Z,"a","b",200),
                  {[{"b",[{"b",0},{"c",0},{"z",0}]}, {"c",[{"b",0},{"c",4},{"z",3}]}, {"z", [{"b",0},{"c",1},{"z",2}]}], [{"a",[{"a",209},{"c",2},{"z",3}]}]}),
    ?assertEqual( retire_peer(W,"b","z",1),
                  {[{"c",[{"c",4},{"d",3},{"z",0}]}, {"d",[{"c",0},{"d",1},{"e",2}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], [{"b",[{"a",9},{"b",3},{"c",3}]}]}),
    ?assertEqual( retire_peer(W,"a","z",10),
                  {[{"b",[{"b",2},{"c",3},{"z",0}]}, {"c",[{"b",1},{"c",4},{"d",3}]}, {"d", [{"c",0},{"d",1},{"e",2}]}], []}).

prune_retired_peers_test() ->
    D1 = [{"a",[1,2,22]}, {"b",[4,5,11]}],
    D2 = [{"a",[1,2,22]}, {"z",[4,5,11]}],
    A = {[{"a",[{"a",0},{"c",0},{"z",0}]}, {"c",[{"a",0},{"c",0},{"z",0}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], [{"b",[{"a",0},{"b",0},{"c",0}]}]},
    A2 = {[{"a",[{"a",0},{"c",0},{"z",0}]}, {"c",[{"a",0},{"c",0},{"z",0}]}, {"z", [{"a",0},{"c",0},{"z",0}]}], []},
    ?assertEqual( prune_retired_peers(A, D1), A),
    ?assertEqual( prune_retired_peers(A, D2), A2),
    ?assertEqual( prune_retired_peers(A, []), A2).



-endif.

