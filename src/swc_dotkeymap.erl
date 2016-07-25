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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("include/swc.hrl").

%% API exports
-export([ new/1
        , empty/1
        , add_owner/2
        , add_dot/4
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

-spec prune(key_matrix(), vv_matrix()) -> {key_matrix(), RemovedKeys :: [id()]}.
prune({{Id, Base, KL}, D}, M) ->
    OwnerMin = swc_watermark:min(M, Id),
    {RemovedKeys, Base2, KL2} =
        case OwnerMin > Base of
            false -> % we don't need to remove any keys from the log
                {[], Base, KL};
            true  -> % we can remove keys and shrink the key_matrix
                {RemKeys, CurrentKeys} = lists:split(OwnerMin - Base, KL),
                {RemKeys, OwnerMin, CurrentKeys}
        end,
    {RemovedKeys2, D2} = orddict:fold(fun (K,V,Acc) ->
                                              ToList = orddict:to_list(V),
                                              Min = swc_watermark:min(M, K),
                                              prune_filter(K, Min, ToList, [], Acc)
                                      end, {[], orddict:new()}, D),
    {{{Id,Base2,KL2}, D2}, RemovedKeys ++ RemovedKeys2}.

-spec prune_filter(id(), counter(), [{counter(), id()}], [{counter(), id()}], {[id()], orddict:orddict()}) -> {[id()], orddict:orddict()}.
prune_filter(_, _, [], [], {R,D}) ->
    {R,D};
prune_filter(Id, _, [], List, {R,D}) ->
    Dict = orddict:from_list(List),
    {R, orddict:store(Id, Dict, D)};
prune_filter(Id, Min, [{C,K}|T], Acc, {RKs,D}) when C =< Min ->
    prune_filter(Id, Min, T, Acc, {[K|RKs],D});
prune_filter(Id, Min, [{C,K}|T], Acc, {RKs,D}) when C > Min ->
    prune_filter(Id, Min, T, [{C,K}|Acc], {RKs,D}).


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
    ?assertEqual( K1 , {{"a", 0, ["k1"]}, []}),
    ?assertEqual( K2 , {{"a", 0, ["k1", "k2"]}, []}),
    ?assertEqual( K3 , {{"a", 0, ["k1", "k2"]}, [{"b",[{4,"kb"}]}]}),
    ?assertEqual( K4 , {{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}]}),
    ?assertEqual( K5 , {{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}).


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
    M2 = swc_watermark:add(M1, "a", {"c",1}),
    M3 = swc_watermark:add(M2, "a", {"a",2}),
    M4 = swc_watermark:add(M3, "a", {"c",2}),
    M5 = swc_watermark:add(M4, "b", {"x",10}),
    M6 = swc_watermark:add(M5, "z", {"c",190}),
    M7 = swc_watermark:add(M6, "c", {"c",200}),
    ?assertEqual( prune(K5, M1) , {{{"a", 0, ["k1", "k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, []}),
    ?assertEqual( prune(K5, M2) , {{{"a", 1, ["k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, ["k1"]}),
    ?assertEqual( prune(K5, M3) , {{{"a", 1, ["k2"]}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, ["k1"]}),
    ?assertEqual( prune(K5, M4) , {{{"a", 2, []}, [{"b",[{2,"kb2"},{4,"kb"}]}, {"c",[{20,"kc"}]}]}, ["k1","k2"]}),
    ?assertEqual( prune(K5, M5) , {{{"a", 2, []}, [{"c",[{20,"kc"}]}]}, ["k1","k2","kb","kb2"]}),
    ?assertEqual( prune(K5, M6) , {{{"a", 2, []}, [{"c",[{20,"kc"}]}]}, ["k1","k2","kb","kb2"]}),
    ?assertEqual( prune(K5, M7) , {{{"a", 2, []}, []}, ["k1","k2","kc","kb","kb2"]}).

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
