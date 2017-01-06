# airports

Fun with [spark].

Looks up the nearest airports from a given airport by distance or by average flight time based on flight data in a csv.

Download the data [here] and unzip it in the root directory, then just type

```
sbt run
```

Don't forget to bump the heap size a bit :)

```
SBT_OPTS="$SBT_OPTS -Xmx4G" sbt run
```

# TODO

It runs for approx. 90s. The exact cause is still unknown, but checking the things around GC might worth a shot. I would expect it to be much faster.
Also, this might be very far from an optimal solution, it is in dire need of optimazations.

## notes

- builds a graph where the vertexes are the airports and the distance and flight time information is recorded on the edges
- parallel edges are then reduced into one, computing the average flight time
- then it just takes a subgraph, where all the edges are going _from_ the airport we're interested in, sorts the triplets and renders a list with the nearest `n` airports
- it wasn't written in TDD, rather in a REPL-style


[spark]: http://spark.apache.org
[here]: http://stat-computing.org/dataexpo/2009/2008.csv.bz2
