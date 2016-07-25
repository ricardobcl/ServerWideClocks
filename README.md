Server Wide Clocks (swc)  [![Build Status](https://travis-ci.org/ricardobcl/ServerWideClocks.svg)](https://travis-ci.org/ricardobcl/ServerWideClocks)
=====

This is an Erlang OTP library for Server Wide Clocks.

Your can find the paper here: http://haslab.uminho.pt/tome/files/global_logical_clocks.pdf

There are 2 main files:

  - the **node clock** -> implemented in swc_node.erl
  - the **key-value clock** -> implemented in swc_kv.erl

Build
-----

```shell
> make all
```
