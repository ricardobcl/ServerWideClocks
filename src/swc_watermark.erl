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
        , add/3
        , add/4
        , min/2
        , peers/1
        , get/3
        , reset_counters/1
        , delete_peer/2
        ]).

-spec new() -> vv_matrix().
new() ->
    orddict:new().

-spec add(vv_matrix(), id(), bvv()) -> vv_matrix().
add(M, EntryId, C) ->
    Peers = case orddict:find(EntryId, M) of
                error -> peers(M);
                {ok, E} -> peers(M) ++ swc_vv:ids(E)
            end,
    VV1 = orddict:map(fun (_,{B,_}) -> B end, C),
    VV2 = orddict:filter(fun (K,_) -> lists:member(K,Peers) end, VV1),
    orddict:update(
        EntryId,
        fun (OldVV) -> swc_vv:join(OldVV, VV2) end,
        VV2, M).


-spec add(vv_matrix(), id(), id(), counter()) -> vv_matrix().
add(M, EntryId, PeerId, Counter) ->
    Top = {PeerId, Counter},
    orddict:update(
        EntryId,
        fun (OldVV) -> swc_vv:add(OldVV, Top) end,
        swc_vv:add(swc_vv:new(), Top),
        M).

-spec min(vv_matrix(), id()) -> counter().
min(M, Id) ->
    case orddict:find(Id, M) of
        error -> 0;
        {ok, VV} -> swc_vv:min(VV)
    end.

-spec peers(vv_matrix()) -> [id()].
peers(M) ->
    orddict:fetch_keys(M).

-spec get(vv_matrix(), id(), id()) -> counter().
get(M, P1, P2) ->
    case orddict:find(P1, M) of
        error -> 0;
        {ok, VV} -> swc_vv:get(P2, VV)
    end.


-spec reset_counters(vv_matrix()) -> vv_matrix().
reset_counters(M) ->
    orddict:map(fun (_Id,VV) -> swc_vv:reset_counters(VV) end, M).

-spec delete_peer(vv_matrix(), id()) -> vv_matrix().
delete_peer(M, Id) ->
    M2 = orddict:erase(Id, M),
    orddict:map(fun (_Id,VV) -> swc_vv:delete_key(VV, Id) end, M2).


%%===================================================================
%% EUnit tests
%%===================================================================

-ifdef(TEST).

add_test() ->
    C1 = [{"a",{12,0}}, {"b",{7,0}}, {"c",{4,0}}, {"d",{5,0}}, {"e",{5,0}}, {"f",{7,10}}, {"g",{5,10}}, {"h",{5,14}}],
    C2 = [{"a",{5,14}}, {"b",{5,14}}, {"c",{50,14}}, {"d",{5,14}}, {"e",{15,0}}, {"f",{5,14}}, {"g",{7,10}}, {"h",{7,10}}],
    M = new(),
    M1 = add(M, "a", "b",4),
    M2 = add(M1, "a", "c",10),
    M3 = add(M2, "c", "c",2),
    M4 = add(M3, "c", "c",20),
    M5 = add(M4, "c", "c",15),
    M6 = add(M5, "c", C1),
    M7 = add(M5, "c", C2),
    M8 = add(M5, "a", C1),
    M9 = add(M5, "a", C2),
    ?assertEqual( M1, [{"a",[{"b",4}]}]),
    ?assertEqual( M2, [{"a",[{"b",4}, {"c",10}]}]),
    ?assertEqual( M3, [{"a",[{"b",4}, {"c",10}]}, {"c",[{"c",2}]}]),
    ?assertEqual( M4, [{"a",[{"b",4}, {"c",10}]}, {"c",[{"c",20}]}]),
    ?assertEqual( M4, M5),
    ?assertEqual( M6, [{"a",[{"b",4},  {"c",10}]},          {"c",[{"a",12}, {"c",20}]}]),
    ?assertEqual( M7, [{"a",[{"b",4},  {"c",10}]},          {"c",[{"a",5},  {"c",50}]}]),
    ?assertEqual( M8, [{"a",[{"a",12}, {"b",7}, {"c",10}]}, {"c",[{"c",20}]}]),
    ?assertEqual( M9, [{"a",[{"a",5},  {"b",5}, {"c",50}]}, {"c",[{"c",20}]}]).


min_test() ->
    M = new(),
    M1 = add(M, "a", "b",4),
    M2 = add(M1, "a", "c",10),
    M3 = add(M2, "c", "c",2),
    M4 = add(M3, "c", "c",20),
    ?assertEqual( min(M, "a"), 0),
    ?assertEqual( min(M1, "a"), 4),
    ?assertEqual( min(M1, "b"), 0),
    ?assertEqual( min(M4, "a"), 4),
    ?assertEqual( min(M4, "c"), 20),
    ?assertEqual( min(M4, "b"), 0).

peers_test() ->
    M = new(),
    M1 = add(M, "a", "b",4),
    M2 = add(M1, "a", "c",10),
    M3 = add(M2, "c", "c",2),
    M4 = add(M3, "c", "c",20),
    M5 = add(M4, "c", "c",15),
    ?assertEqual( peers(M), []),
    ?assertEqual( peers(M1), ["a"]),
    ?assertEqual( peers(M5), ["a", "c"]).


get_test() ->
    M = new(),
    M1 = add(M, "a", "b",4),
    M2 = add(M1, "a", "c",10),
    M3 = add(M2, "c", "c",2),
    M4 = add(M3, "c", "c",20),
    ?assertEqual( get(M, "a", "a"), 0),
    ?assertEqual( get(M1, "a", "a"), 0),
    ?assertEqual( get(M1, "b", "a"), 0),
    ?assertEqual( get(M4, "c", "c"), 20),
    ?assertEqual( get(M4, "a", "c"), 10).

reset_counters_test() ->
    M = new(),
    M1 = add(M, "a", "b",4),
    M2 = add(M1, "a", "c",10),
    M3 = add(M2, "c", "c",2),
    M4 = add(M3, "c", "c",20),
    ?assertEqual( reset_counters(M), M),
    ?assertEqual( reset_counters(M1), [{"a",[{"b",0}]}]),
    ?assertEqual( reset_counters(M2), [{"a",[{"b",0}, {"c",0}]}]),
    ?assertEqual( reset_counters(M3), [{"a",[{"b",0}, {"c",0}]}, {"c",[{"c",0}]}]),
    ?assertEqual( reset_counters(M4), [{"a",[{"b",0}, {"c",0}]}, {"c",[{"c",0}]}]).

delete_peer_test() ->
    M = new(),
    M1 = add(M, "a", "b",4),
    M2 = add(M1, "a", "c",10),
    M3 = add(M2, "c", "c",2),
    M4 = add(M3, "c", "c",20),
    ?assertEqual( delete_peer(M1, "a"), []),
    ?assertEqual( delete_peer(M1, "b"), [{"a",[]}]),
    ?assertEqual( delete_peer(M1, "c"), [{"a",[{"b",4}]}]),
    ?assertEqual( delete_peer(M4, "a"), [{"c",[{"c",20}]}]),
    ?assertEqual( delete_peer(M4, "c"), [{"a",[{"b",4}]}]).

-endif.

