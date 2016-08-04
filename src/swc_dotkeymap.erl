%%%-------------------------------------------------------------------
%%% @author Ricardo Gonçalves <tome.wave@gmail.com>
%%% @copyright (C) 2016, Ricardo Gonçalves
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(swc_dotkeymap).

-author('Ricardo Gonçalves <tome.wave@gmail.com>').

-compile({no_auto_import,[size/1]}).

-include_lib("swc/include/swc.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API exports
-export([ new/0
        , empty/1
        , add_key/4
        , add_objects/2
        , size/1
        , size/2
        , prune/2
        , get_keys/2
        ]).

-spec new() -> key_matrix().
new() ->
    orddict:new().

-spec empty(key_matrix()) -> boolean().
empty(D) ->
    size(D) == 0.


-spec add_key(key_matrix(), id(), id(), counter()) -> key_matrix().
add_key(D, Id, Key, Counter) ->
    orddict:update( Id,
                    fun (OldOrdDict) -> orddict:store(Counter, Key, OldOrdDict) end,
                    orddict:store(Counter, Key, orddict:new()),
                    D).

-spec add_objects(key_matrix(), [{Key::id(), Object::dcc()}]) -> key_matrix().
add_objects(DKM, []) -> DKM;
add_objects(DKM, [{Key,Obj}|T]) ->
    {Dots,_} = Obj,
    DKM2 = orddict:fold(
             fun (_Dot={Id,Counter},_,Acc) ->
                     add_key(Acc, Id, Key, Counter)
             end,
             DKM, Dots),
    add_objects(DKM2, T).


-spec size(key_matrix()) -> non_neg_integer().
size(D) ->
    orddict:fold(fun (_K,V,Acc) -> Acc + orddict:size(V) end, 0, D).

-spec size(key_matrix(), id()) -> non_neg_integer().
size(D, Id) ->
    case orddict:find(Id, D) of
        error -> 0;
        {ok, V} -> orddict:size(V)
    end.

-spec prune(key_matrix(), vv_matrix()) -> {key_matrix(), RemovedKeys :: [{id(), [{counter(),id()}]}]}.
prune(D, M) ->
    orddict:fold(
        fun (Peer, V, {KeepDic, RemoveDic}) ->
            Min = swc_watermark:min(M, Peer),
            Keep   = orddict:filter(fun (Counter,_) -> Counter  > Min end, V),
            Remove = orddict:filter(fun (Counter,_) -> Counter =< Min end, V),
            case {orddict:is_empty(Keep), orddict:is_empty(Remove)} of
                {true ,true}  -> {KeepDic,                               RemoveDic};
                {false,true}  -> {orddict:store(Peer, Keep, KeepDic),    RemoveDic};
                {true ,false} -> {KeepDic,                               orddict:store(Peer, Remove, RemoveDic)};
                {false,false} -> {orddict:store(Peer, Keep, KeepDic),    orddict:store(Peer, Remove, RemoveDic)}
            end
            % prune_filter(K, Min, ToList, [], Acc)
        end,
        {orddict:new(), orddict:new()}, D).


-spec get_keys(key_matrix(), [{id(),[counter()]}]) -> {FoundKeys::[id()], MissingKeys::[{id(), [counter()]}]}.
get_keys(D, L) -> get_keys(D, L, {[],[]}).

get_keys(_, [], Acc) -> Acc;
get_keys(D, [{Id, Dots}|T], {FoundKeys, MissingKeys}) ->
    Acc2 = case orddict:find(Id, D) of
        error ->
            {FoundKeys, [{Id,Dots}|MissingKeys]};
        {ok, DotKey} ->
            case get_keys_aux(DotKey, Dots) of
                {FK, []} -> {FK ++ FoundKeys, MissingKeys};
                {FK, MK} -> {FK ++ FoundKeys, [{Id,MK}|MissingKeys]}
            end
    end,
    get_keys(D, T, Acc2).

-spec get_keys_aux(orddict:orddict(), [counter()]) -> {Found::[id()], NotFound::[counter()]}.
get_keys_aux(O, L) ->
    lists:foldl(
        fun (Dot, {FK, MK}) ->
            case orddict:find(Dot, O) of
                error       -> {FK, [Dot|MK]};
                {ok, Key}   -> {[Key|FK], MK}
            end
        end, {[],[]}, L).


%%===================================================================
%% EUnit tests
%%===================================================================

-ifdef(TEST).

add_test() ->
    K1 = add_key(new(), "a", "k1", 1),
    K2 = add_key(K1, "a", "k2", 2),
    K3 = add_key(K2, "b", "kb", 4),
    K4 = add_key(K3, "b", "kb2", 2),
    K5 = add_key(K4, "c", "kc", 20),
    O1 = { [{{"z",8}, "red"}, {{"z",11}, "purple"}, {{"b",3}, "green"}] , [{"a",11}] },
    O2 = { [{{"b",11}, "purple"}] , [{"a",4}, {"b",20}] },
    K6 = add_objects(K1, [{"k100",O1}, {"k200",O2}]),
    K7 = add_objects(K1, [{"k200",O2}, {"k100",O1}]),
    K8 = add_objects(K5, [{"k200",O2}, {"k100",O1}]),
    ?assertEqual( K1 , [{"a", [{1, "k1"}]}]),
    ?assertEqual( K2 , [{"a", [{1, "k1"}, {2, "k2"}]}]),
    ?assertEqual( K3 , [{"a", [{1, "k1"}, {2, "k2"}]}, {"b",[{4,"kb"}]}]),
    ?assertEqual( K4 , [{"a", [{1, "k1"}, {2, "k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}]),
    ?assertEqual( K5 , [{"a", [{1, "k1"}, {2, "k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]),
    ?assertEqual( K6 , [{"a", [{1, "k1"}]}, {"b",[{3,"k100"},{11,"k200"}]}, {"z",[{8,"k100"}, {11,"k100"}]}]),
    ?assertEqual( K6 , K7),
    ?assertEqual( K8 , [{"a", [{1, "k1"}, {2, "k2"}]}, {"b",[{2,"kb2"},{3,"k100"},{4,"kb"},{11,"k200"}]}, {"c",[{20,"kc"}]}, {"z",[{8,"k100"}, {11,"k100"}]}]).


empty_test() ->
    K1 = add_key(new(), "a", "k1", 1),
    K2 = add_key(K1, "a", "k2", 2),
    K3 = add_key(K2, "b", "kb", 4),
    K4 = add_key(K3, "b", "kb2", 2),
    ?assertEqual( empty(new()), true),
    ?assertEqual( empty(K1), false),
    ?assertEqual( empty(K2), false),
    ?assertEqual( empty(K3), false),
    ?assertEqual( empty(K4), false).

size_test() ->
    K1 = add_key(new(), "a", "k1", 1),
    K2 = add_key(K1, "a", "k2", 2),
    K3 = add_key(K2, "b", "kb", 4),
    K4 = add_key(K3, "b", "kb2", 2),
    K5 = add_key(K4, "c", "kc", 20),
    ?assertEqual( size(K1) , 1),
    ?assertEqual( size(K2) , 2),
    ?assertEqual( size(K3) , 3),
    ?assertEqual( size(K4) , 4),
    ?assertEqual( size(K5) , 5),
    ?assertEqual( size(K5, "a") , 2),
    ?assertEqual( size(K5, "b") , 2),
    ?assertEqual( size(K5, "c") , 1),
    ?assertEqual( size(K5, "d") , 0).

prune_test() ->
    K1 = add_key(new(), "a", "k1", 1),
    K2 = add_key(K1, "a", "k2", 2),
    K3 = add_key(K2, "b", "kb", 4),
    K4 = add_key(K3, "b", "kb2", 2),
    K5 = add_key(K4, "c", "kc", 20),
    M1 = swc_watermark:new(),
    M2 = swc_watermark:update_cell(M1, "a", "c",1),
    M3 = swc_watermark:update_cell(M2, "a", "a",2),
    M4 = swc_watermark:update_cell(M3, "a", "c",2),
    M5 = swc_watermark:update_cell(M4, "b", "x",10),
    M6 = swc_watermark:update_cell(M5, "z", "c",190),
    M7 = swc_watermark:update_cell(M6, "c", "c",200),
    ?assertEqual( prune(K5, M1) , {[{"a", [{1, "k1"}, {2, "k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}], []}),
    ?assertEqual( prune(K5, M2) , {[{"a", [{2, "k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}], [{"a",[{1,"k1"}]}]}),
    ?assertEqual( prune(K5, M3) , {[{"a", [{2, "k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}], [{"a",[{1,"k1"}]}]}),
    ?assertEqual( prune(K5, M4) , {[{"b", [{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}], [{"a",[{1,"k1"},{2,"k2"}]}]}),
    ?assertEqual( prune(K5, M5) , {[{"c", [{20,"kc"}]}], [{"a",[{1,"k1"}, {2,"k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}]}),
    ?assertEqual( prune(K5, M6) , {[{"c", [{20,"kc"}]}], [{"a",[{1,"k1"}, {2,"k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}]}),
    ?assertEqual( prune(K5, M7) , {[], [{"a",[{1,"k1"}, {2,"k2"}]}, {"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}).

get_keys_test() ->
    K1 = add_key(new(), "a", "k1", 1),
    K2 = add_key(K1, "a", "k2", 2),
    K3 = add_key(K2, "b", "kb", 4),
    K4 = add_key(K3, "b", "kb2", 2),
    K5 = add_key(K4, "c", "kc", 20),
    ?assertEqual( get_keys(K5,[{"a",[1]}]), {["k1"],[]}),
    ?assertEqual( get_keys(K5,[{"a",[2]}]), {["k2"],[]}),
    ?assertEqual( get_keys(K5,[{"a",[1,2]}]), {["k2","k1"],[]}),
    ?assertEqual( get_keys(K5,[{"a",[1,2,5,6]}]), {["k2","k1"],[{"a",[6,5]}]}),
    ?assertEqual( get_keys(K5,[{"a",[1,2]}, {"b",[4,5,11]}]), {["kb","k2","k1"],[{"b",[11,5]}]}),
    ?assertEqual( get_keys(K5,[{"a",[1,2,22]}, {"b",[4,5,11]}]), {["kb","k2","k1"],[{"b",[11,5]}, {"a",[22]}]}),
    ?assertEqual( get_keys(K1,[{"a",[2]}, {"b",[4,5]}]), {[],[{"b",[4,5]}, {"a",[2]}]}).

-endif.
