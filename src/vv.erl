%% @author Ricardo Gonçalves <tome.wave@gmail.com>
%%
%% @doc  
%% An Erlang implementation of a Version Vector.
%% @end  

-module(vv).
-author('Ricardo Gonçalves <tome.wave@gmail.com>').

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("include/glc.hrl").

-export([ new/0
        , ids/1
        , get/2
        , join/2
        , filter/2
        , add/2
        , min/1
        , min_key/1
        , reset_with_same_ids/1
        , delete_key/2
        ]).

-export_type([vv/0]).

%% @doc Initializes a new empty version vector.
-spec new() -> vv().
new() -> orddict:new().

%% @doc Returns all the keys (ids) from a VV.
-spec ids(vv()) -> [id()].
ids(V) ->
    orddict:fetch_keys(V).

%% @doc Returns the counter associated with an id K. If the key is not present
%% in the VV, it returns 0.
-spec get(id(), vv()) -> {ok, counter()} | error.
get(K,V) ->
    case orddict:find(K,V) of
        error   -> 0;
        {ok, C} -> C
    end.

%% @doc Merges or joins two VVs, taking the maximum counter if an entry is
%% present in both VVs.
-spec join(vv(), vv()) -> vv().
join(A,B) ->
    FunMerge = fun (_Id, C1, C2) -> max(C1, C2) end,
    orddict:merge(FunMerge, A, B).

%% @doc It applies some boolean function F to all entries in the VV, removing
%% those that return False when F is used.
-spec filter(fun((id(), counter()) -> boolean()), vv()) -> vv().
filter(F,V) ->
    orddict:filter(F, V).

%% @doc Adds an entry {Id, Counter} to the VV, performing the maximum between
%% both counters, if the entry already exists.
-spec add(vv(), {id(), counter()}) -> vv().
add(VV, {Id, Counter}) ->
    Fun = fun (C) -> max(C, Counter) end,
    orddict:update(Id, Fun, Counter, VV).

%% @doc Returns the minimum counters from all the entries in the VV.
-spec min(vv()) -> counter().
min(VV) ->
    Keys = orddict:fetch_keys(VV),
    Values = [orddict:fetch(Key, VV) || Key <- Keys],
    lists:min(Values).

%% @doc Returns the key with the minimum counter associated.
-spec min_key(vv()) -> id().
min_key(VV) ->
    Fun = fun (Key, Value, {MKey, MVal}) ->
            case Value < MVal of
                true  -> {Key, Value};
                false -> {MKey, MVal}
            end
        end,
    [Head | Tail] = VV,
    {MinKey, _MinValue} = orddict:fold(Fun, Head, Tail),
    MinKey.

%% @doc Returns the VV with the same entries, but with counters at zero.
-spec reset_with_same_ids(vv()) -> vv().
reset_with_same_ids(VV) ->
    IDs = vv:ids(VV),
    lists:foldl(fun(Id, Acc) -> vv:add(Acc, {Id,0}) end, vv:new(), IDs).


%% @doc Returns the VV without the entry with a given key.
-spec delete_key(vv(), id()) -> vv().
delete_key(VV, Key) ->
    orddict:erase(Key, VV).

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

min_key_test() ->
    A0 = [{"a",2}],
    A1 = [{"a",2}, {"b",4}, {"c",4}],
    A2 = [{"a",5}, {"b",4}, {"c",4}],
    A3 = [{"a",4}, {"b",4}, {"c",4}],
    A4 = [{"a",5}, {"b",14}, {"c",4}],
    ?assertEqual( "a", min_key(A0)),
    ?assertEqual( "a", min_key(A1)),
    ?assertEqual( "b", min_key(A2)),
    ?assertEqual( "a", min_key(A3)),
    ?assertEqual( "c", min_key(A4)),
    ok.

reset_with_same_ids_test() ->
    E = [],
    A0 = [{"a",2}],
    A1 = [{"a",2}, {"b",4}, {"c",4}],
    ?assertEqual(reset_with_same_ids(E), []),
    ?assertEqual(reset_with_same_ids(A0), [{"a",0}]),
    ?assertEqual(reset_with_same_ids(A1), [{"a",0}, {"b",0}, {"c",0}]),
    ok.

delete_key_test() ->
    E = [],
    A0 = [{"a",2}],
    A1 = [{"a",2}, {"b",4}, {"c",4}],
    ?assertEqual(delete_key(E, "a"), []),
    ?assertEqual(delete_key(A0, "a"), []),
    ?assertEqual(delete_key(A0, "b"), [{"a",2}]),
    ?assertEqual(delete_key(A1, "a"), [{"b",4}, {"c",4}]),
    ok.


-endif.
