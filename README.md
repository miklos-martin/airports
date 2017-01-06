# airports

Fun with [spark].

Looks up the nearest airports from a given airport by distance or by average flight time based on flight data in csv.

Download the data [here] and unzip it in the root directory, then just type

```
sbt run
```

Don't forget to bump the heap size a bit :)

```
SBT_OPTS="$SBT_OPTS -Xmx4G" sbt run
```

# TODO

It runs for approx. 90s. The exact cause is still unknown, but checking the things around GC might worth a shot.
Also, this might be very far from an optimal solution, it is in dire need of optimazations.

[spark]: http://spark.apache.org
[here]: http://stat-computing.org/dataexpo/2009/2008.csv.bz2
