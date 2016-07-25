
-type vv()      :: [{Key :: id(), Entry :: counter()}]. % orddict().

-type dcc()     :: {dots(), vv()}.

-type bvv()     :: [{Key :: id(), Entry :: entry()}]. % orddict().

-type key_matrix()  :: {{id(), counter(), [id()]}, orddict:orddict()}.

-type vv_matrix() :: orddict:orddict().

-type dots()    :: [{dot(), value()}]. % orddict(dot -> value).
-type dot()     :: {id(), counter()}.
-type entry()   :: {counter(), counter()}.
-type id()      :: term().
-type counter() :: non_neg_integer().
-type value()   :: any().
