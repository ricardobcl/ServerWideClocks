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
-export([ new/1
        , empty/1
        , add_owner/2
        , add_dot/4
        , add_objects/2
        , size/1
        , size/2
        , prune/2
        , get_keys/2
        ]).

-spec new(id()) -> key_matrix().
new(OwnerID) ->
    {{OwnerID, 0, []}, orddict:new()}.

-spec empty(key_matrix()) -> boolean().
empty({O,D}) ->
    case {O, orddict:size(D)} of
        {{_,_,[]}, 0} -> true;
        _ -> false
    end.


-spec add_owner(key_matrix(), id()) -> key_matrix().
add_owner({{Id, C, L}, D}, Key) ->
    {{Id, C, L++[Key]}, D}.

-spec add_dot(key_matrix(), id(), id(), counter()) -> key_matrix().
add_dot({O,D}, Id, Key, Counter) ->
    D2 = orddict:update(Id,
                        fun (OldOrdDict) -> orddict:store(Counter, Key, OldOrdDict) end,
                        orddict:store(Counter, Key, orddict:new()),
                        D),
    {O, D2}.


-spec add_objects(key_matrix(), [{Key::id(), Object::dcc()}]) -> key_matrix().
add_objects(DKM, []) -> DKM;
add_objects(DKM, [{Key,Obj}|T]) ->
    {Dots,_} = Obj,
    DKM2 = orddict:fold(
             fun (_Dot={Id,Counter},_,Acc) ->
                     add_dot(Acc, Id, Key, Counter)
             end,
             DKM, Dots),
    add_objects(DKM2, T).



-spec size(key_matrix()) -> non_neg_integer().
size({{_,_,L},D}) ->
    length(L) + orddict:fold(fun (_K,V,Acc) -> Acc + orddict:size(V) end, 0, D).

-spec size(key_matrix(), id()) -> non_neg_integer().
size({{Id,_,L},_}, Id) ->
    length(L);
size({_,D}, Id) ->
    case orddict:find(Id, D) of
        error -> 0;
        {ok, V} -> orddict:size(V)
    end.

-spec prune(key_matrix(), vv_matrix()) -> {key_matrix(), RemovedKeys :: [{id(), [{counter(),id()}]}]}.
prune({O,D}, M) ->
    {O2, R1} = prune_owner(O,M),
    {D2, R2} = prune_peers(D,M),
    {{O2,D2}, R1 ++ R2}.

prune_owner(O={Id, Base, KL}, M) ->
    OwnerMin = swc_watermark:min(M, Id),
    case OwnerMin > Base of
        false -> % we don't need to remove any keys from the owner log
            {O, []};
        true  -> % we can remove keys and shrink the owner log
            {RemovedKeys, CurrentKeys} = lists:split(OwnerMin - Base, KL),
            {_,RemovedKeys2} = lists:foldl(fun (Key, {B,Res}) -> {B+1, [{B+1,Key}|Res]} end, {Base,[]}, RemovedKeys),
            {{Id, OwnerMin, CurrentKeys}, [{Id, RemovedKeys2}]}
    end.

prune_peers(D,M) ->
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


-spec get_keys(key_matrix(), [{id(),[counter()]}]) -> [id()].
get_keys(K, L) -> get_keys(K, L, []).

get_keys(_, [], Acc) -> Acc;
get_keys(A={{OwnerId, Base, KL}, _}, [{Id, Dots}|T], Acc) when OwnerId == Id ->
    MissingKeys = [lists:nth(Dot-Base, KL) || Dot <- Dots, Dot > Base, Dot =< Base + length(KL)],
    get_keys(A, T, MissingKeys ++ Acc);
get_keys(A={{OwnerId, _, _}, D}, [{Id, Dots}|T], Acc) when OwnerId =/= Id ->
    MissingKeys = case orddict:find(Id, D) of
        error -> [];
        {ok, O} ->
            orddict:fold(fun (K,V,Acc2) ->
                            case lists:member(K,Dots) of
                                true -> [V|Acc2];
                                false -> Acc2
                            end
                        end, [], O)
    end,
    get_keys(A, T, Acc ++ MissingKeys).



%%===================================================================
%% EUnit tests
%%===================================================================

-ifdef(TEST).

new_test() ->
    ?assertEqual( new("a"), {{"a",0,[]},[]}).

add_test() ->
    K1 = add_owner(new("a"), "k1"),
    K2 = add_owner(K1, "k2"),
    K3 = add_dot(K2, "b", "kb", 4),
    K4 = add_dot(K3, "b", "kb2", 2),
    K5 = add_dot(K4, "c", "kc", 20),
    O1 = { [{{"z",8}, "red"}, {{"z",11}, "purple"}, {{"b",3}, "green"}] , [{"a",11}] },
    O2 = { [{{"b",11}, "purple"}] , [{"a",4}, {"b",20}] },
    K6 = add_objects(K1, [{"k100",O1}, {"k200",O2}]),
    K7 = add_objects(K1, [{"k200",O2}, {"k100",O1}]),
    K8 = add_objects(K5, [{"k200",O2}, {"k100",O1}]),
    ?assertEqual( K1 , {{"a", 0, ["k1"]}, []}),
    ?assertEqual( K2 , {{"a", 0, ["k1", "k2"]}, []}),
    ?assertEqual( K3 , {{"a", 0, ["k1", "k2"]}, [{"b",[{4,"kb"}]}]}),
    ?assertEqual( K4 , {{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}]}),
    ?assertEqual( K5 , {{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}),
    ?assertEqual( K6 , {{"a", 0, ["k1"]}, [{"b",[{3,"k100"},{11,"k200"}]}, {"z",[{8,"k100"}, {11,"k100"}]}]}),
    ?assertEqual( K6 , K7),
    ?assertEqual( K8 , {{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{3,"k100"},{4,"kb"},{11,"k200"}]}, {"c",[{20,"kc"}]}, {"z",[{8,"k100"}, {11,"k100"}]}]}).


empty_test() ->
    K1 = add_owner(new("a"), "k1"),
    K2 = add_owner(K1, "k2"),
    K3 = add_dot(K2, "b", "kb", 4),
    K4 = add_dot(K3, "b", "kb2", 2),
    ?assertEqual( empty(new("a")), true),
    ?assertEqual( empty({{"a", 10, []}, []}), true),
    ?assertEqual( empty(K1), false),
    ?assertEqual( empty(K2), false),
    ?assertEqual( empty(K3), false),
    ?assertEqual( empty(K4), false).

size_test() ->
    K1 = add_owner(new("a"), "k1"),
    K2 = add_owner(K1, "k2"),
    K3 = add_dot(K2, "b", "kb", 4),
    K4 = add_dot(K3, "b", "kb2", 2),
    K5 = add_dot(K4, "c", "kc", 20),
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
    K1 = add_owner(new("a"), "k1"),
    K2 = add_owner(K1, "k2"),
    K3 = add_dot(K2, "b", "kb", 4),
    K4 = add_dot(K3, "b", "kb2", 2),
    K5 = add_dot(K4, "c", "kc", 20),
    M1 = swc_watermark:new(),
    M2 = swc_watermark:update_cell(M1, "a", "c",1),
    M3 = swc_watermark:update_cell(M2, "a", "a",2),
    M4 = swc_watermark:update_cell(M3, "a", "c",2),
    M5 = swc_watermark:update_cell(M4, "b", "x",10),
    M6 = swc_watermark:update_cell(M5, "z", "c",190),
    M7 = swc_watermark:update_cell(M6, "c", "c",200),
    ?assertEqual( prune(K5, M1) , {{{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, []}),
    ?assertEqual( prune(K5, M2) , {{{"a", 1, ["k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, [{"a",[{1,"k1"}]}]}),
    ?assertEqual( prune(K5, M3) , {{{"a", 1, ["k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, [{"a",[{1,"k1"}]}]}),
    ?assertEqual( prune(K5, M4) , {{{"a", 2, []}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, [{"a",[{2,"k2"}, {1,"k1"}]}]}),
    ?assertEqual( prune(K5, M5) , {{{"a", 2, []}, [{"c",[{20,"kc"}]}]}, [{"a",[{2,"k2"}, {1,"k1"}]}, {"b",[{2,"kb2"},{4,"kb"}]}]}),
    ?assertEqual( prune(K5, M6) , {{{"a", 2, []}, [{"c",[{20,"kc"}]}]}, [{"a",[{2,"k2"}, {1,"k1"}]}, {"b",[{2,"kb2"},{4,"kb"}]}]}),
    ?assertEqual( prune(K5, M7) , {{{"a", 2, []}, []}, [{"a",[{2,"k2"}, {1,"k1"}]}, {"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}).

get_keys_test() ->
    K1 = add_owner(new("a"), "k1"),
    K2 = add_owner(K1, "k2"),
    K3 = add_dot(K2, "b", "kb", 4),
    K4 = add_dot(K3, "b", "kb2", 2),
    K5 = add_dot(K4, "c", "kc", 20),
    ?assertEqual( get_keys(K5,[{"a",[1]}]), ["k1"]),
    ?assertEqual( get_keys(K5,[{"a",[2]}]), ["k2"]),
    ?assertEqual( get_keys(K5,[{"a",[1,2]}]), ["k1","k2"]),
    ?assertEqual( get_keys(K5,[{"a",[1,2]}, {"b",[4,5]}]), ["k1","k2","kb"]),
    ?assertEqual( get_keys(K1,[{"a",[2]}, {"b",[4,5]}]), []),
    ?assertEqual( get_keys(K1,[{"a",[1,2]}, {"b",[4,5]}]), ["k1"]).

-endif.
