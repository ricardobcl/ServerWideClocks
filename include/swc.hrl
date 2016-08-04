
-type vv()      :: [{Key :: id(), Entry :: counter()}]. % orddict().

-type dcc()     :: {dots(), vv()}.

-type bvv()     :: [{Key :: id(), Entry :: entry()}]. % orddict().

-type key_matrix()  :: [{Node :: id(), DotKey :: [{counter(), id()}]}]. % orddict().

-type vv_matrix() :: orddict:orddict().

-type dots()    :: [{dot(), value()}]. % orddict(dot -> value).
-type dot()     :: {id(), counter()}.
-type entry()   :: {counter(), counter()}.
-type id()      :: term().
-type counter() :: non_neg_integer().
-type value()   :: any().
