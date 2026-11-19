---- MODULE SessionRead_TTrace_1795114157 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, SessionRead

_expression ==
    LET SessionRead_TEExpression == INSTANCE SessionRead_TEExpression
    IN SessionRead_TEExpression!expression
----

_trace ==
    LET SessionRead_TETrace == INSTANCE SessionRead_TETrace
    IN SessionRead_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        next = (3)
        /\
        cur = (0)
        /\
        rest = (<<>>)
        /\
        res = (<<"data", "">>)
        /\
        tail = (<<<<>>, <<>>>>)
        /\
        dl = (FALSE)
        /\
        cnt = (<<2, 1, 0, 0>>)
        /\
        got = (<<<<1>>, <<1, 2>>>>)
        /\
        q = (<<>>)
        /\
        ref = (<<1, 1>>)
        /\
        pc = (<<"cut", "rel-rest">>)
        /\
        rel = (<<0, 0>>)
        /\
        lock = (0)
        /\
        closed = (FALSE)
    )
----

_init ==
    /\ tail = _TETrace[1].tail
    /\ cur = _TETrace[1].cur
    /\ q = _TETrace[1].q
    /\ pc = _TETrace[1].pc
    /\ rest = _TETrace[1].rest
    /\ dl = _TETrace[1].dl
    /\ lock = _TETrace[1].lock
    /\ ref = _TETrace[1].ref
    /\ rel = _TETrace[1].rel
    /\ res = _TETrace[1].res
    /\ next = _TETrace[1].next
    /\ cnt = _TETrace[1].cnt
    /\ got = _TETrace[1].got
    /\ closed = _TETrace[1].closed
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ tail  = _TETrace[i].tail
        /\ tail' = _TETrace[j].tail
        /\ cur  = _TETrace[i].cur
        /\ cur' = _TETrace[j].cur
        /\ q  = _TETrace[i].q
        /\ q' = _TETrace[j].q
        /\ pc  = _TETrace[i].pc
        /\ pc' = _TETrace[j].pc
        /\ rest  = _TETrace[i].rest
        /\ rest' = _TETrace[j].rest
        /\ dl  = _TETrace[i].dl
        /\ dl' = _TETrace[j].dl
        /\ lock  = _TETrace[i].lock
        /\ lock' = _TETrace[j].lock
        /\ ref  = _TETrace[i].ref
        /\ ref' = _TETrace[j].ref
        /\ rel  = _TETrace[i].rel
        /\ rel' = _TETrace[j].rel
        /\ res  = _TETrace[i].res
        /\ res' = _TETrace[j].res
        /\ next  = _TETrace[i].next
        /\ next' = _TETrace[j].next
        /\ cnt  = _TETrace[i].cnt
        /\ cnt' = _TETrace[j].cnt
        /\ got  = _TETrace[i].got
        /\ got' = _TETrace[j].got
        /\ closed  = _TETrace[i].closed
        /\ closed' = _TETrace[j].closed

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("SessionRead_TTrace_1795114157.json", _TETrace)

=============================================================================

 Note that you can extract this module `SessionRead_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `SessionRead_TEExpression.tla` file takes precedence 
  over the module `SessionRead_TEExpression` below).

---- MODULE SessionRead_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, SessionRead

expression == 
    [
        \* To hide variables of the `SessionRead` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        tail |-> tail
        ,cur |-> cur
        ,q |-> q
        ,pc |-> pc
        ,rest |-> rest
        ,dl |-> dl
        ,lock |-> lock
        ,ref |-> ref
        ,rel |-> rel
        ,res |-> res
        ,next |-> next
        ,cnt |-> cnt
        ,got |-> got
        ,closed |-> closed
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_tailUnchanged |-> tail = tail'
        
        \* Format the `tail` variable as Json value.
        \* ,_tailJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(tail)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_tailModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].tail # _TETrace[s-1].tail
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE SessionRead_TETrace ----
\*EXTENDS IOUtils, TLC, SessionRead
\*
\*trace == IODeserialize("SessionRead_TTrace_1795114157.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE SessionRead_TETrace ----
EXTENDS TLC, SessionRead

trace == 
    <<
    ([next |-> 1,cur |-> 0,rest |-> <<>>,res |-> <<"", "">>,tail |-> <<<<>>, <<>>>>,dl |-> FALSE,cnt |-> <<0, 0, 0, 0>>,got |-> <<<<>>, <<>>>>,q |-> <<>>,ref |-> <<0, 0>>,pc |-> <<"idle", "idle">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE]),
    ([next |-> 3,cur |-> 0,rest |-> <<>>,res |-> <<"", "">>,tail |-> <<<<>>, <<>>>>,dl |-> FALSE,cnt |-> <<0, 0, 0, 0>>,got |-> <<<<>>, <<>>>>,q |-> <<<<1, 2>>>>,ref |-> <<0, 0>>,pc |-> <<"idle", "idle">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE]),
    ([next |-> 3,cur |-> 0,rest |-> <<>>,res |-> <<"", "">>,tail |-> <<<<>>, <<>>>>,dl |-> FALSE,cnt |-> <<0, 0, 0, 0>>,got |-> <<<<>>, <<>>>>,q |-> <<<<1, 2>>>>,ref |-> <<0, 0>>,pc |-> <<"pop", "idle">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE]),
    ([next |-> 3,cur |-> 1,rest |-> <<1, 2>>,res |-> <<"", "">>,tail |-> <<<<>>, <<>>>>,dl |-> FALSE,cnt |-> <<0, 0, 0, 0>>,got |-> <<<<>>, <<>>>>,q |-> <<>>,ref |-> <<1, 0>>,pc |-> <<"copy", "idle">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE]),
    ([next |-> 3,cur |-> 1,rest |-> <<1, 2>>,res |-> <<"data", "">>,tail |-> <<<<>>, <<>>>>,dl |-> FALSE,cnt |-> <<1, 0, 0, 0>>,got |-> <<<<1>>, <<>>>>,q |-> <<>>,ref |-> <<1, 0>>,pc |-> <<"cut", "idle">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE]),
    ([next |-> 3,cur |-> 0,rest |-> <<>>,res |-> <<"data", "">>,tail |-> <<<<>>, <<1, 2>>>>,dl |-> FALSE,cnt |-> <<1, 0, 0, 0>>,got |-> <<<<1>>, <<>>>>,q |-> <<>>,ref |-> <<1, 1>>,pc |-> <<"cut", "write-rest">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE]),
    ([next |-> 3,cur |-> 0,rest |-> <<>>,res |-> <<"data", "">>,tail |-> <<<<>>, <<>>>>,dl |-> FALSE,cnt |-> <<2, 1, 0, 0>>,got |-> <<<<1>>, <<1, 2>>>>,q |-> <<>>,ref |-> <<1, 1>>,pc |-> <<"cut", "rel-rest">>,rel |-> <<0, 0>>,lock |-> 0,closed |-> FALSE])
    >>
----


=============================================================================

---- CONFIG SessionRead_TTrace_1795114157 ----
CONSTANTS
    Readers = { 1 , 2 }
    Slices = 2
    Copier = 2
    Mutant = "no-read-lock"

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Fri Nov 20 00:19:18 IST 2026