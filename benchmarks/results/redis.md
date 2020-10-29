## Redis Benchmark

### 2020/10

*1Fork 1Thread*

*monix-redis*
```
Benchmark                                                 Mode  Cnt     Score      Error  Units
RedisHashesBenchmark.hashAllReader                       thrpt    5  1740.611 ±  297.286  ops/s
RedisHashesBenchmark.hashFieldValueReader                thrpt    5  1766.194 ±  285.917  ops/s
RedisKeysBenchmark.keyExistsReader                       thrpt    5  1742.423 ±  272.749  ops/s
RedisKeysBenchmark.keyPttlReader                         thrpt    5  1794.874 ±  139.606  ops/s
RedisListsBenchmark.listByIndexReader                    thrpt    5  1742.244 ±  258.268  ops/s
RedisListsBenchmark.listLengthReader                     thrpt    5  1744.402 ±  153.538  ops/s
RedisListsBenchmark.listRangeReader                      thrpt    5  1710.435 ±  374.534  ops/s
RedisSetsBenchmark.setCardReader                         thrpt    5  1629.605 ± 1487.577  ops/s
RedisSetsBenchmark.setDiffWriter                         thrpt    5  1753.346 ±   63.737  ops/s
RedisSetsBenchmark.setMembersReader                      thrpt    5  1753.184 ±  219.565  ops/s
RedisSortedSetsBenchmark.sortedSetCardReader             thrpt    5  1835.319 ±  178.780  ops/s
RedisSortedSetsBenchmark.sortedSetCountReader            thrpt    5  1791.881 ±  164.661  ops/s
RedisSortedSetsBenchmark.sortedSetRangeReader            thrpt    5  1772.455 ±  211.682  ops/s
RedisStringsBenchmark.stringAppender                     thrpt    5  1683.048 ±  103.803  ops/s
RedisStringsBenchmark.stringReader                       thrpt    5  1778.631 ±  216.371  ops/s
RedisWriterBenchmark.hashWriter                          thrpt    5  1694.120 ±  231.674  ops/s
RedisWriterBenchmark.listWriter                          thrpt    5  1654.497 ±  100.961  ops/s
RedisWriterBenchmark.setWriter                           thrpt    5  1637.311 ±  643.775  ops/s
RedisWriterBenchmark.sortedSetWriter                     thrpt    5  1446.247 ±  886.432  ops/s
RedisWriterBenchmark.stringWriter                        thrpt    5  1660.682 ±  71.679  ops/s
```

*redis4cats*
```
Benchmark                                                 Mode  Cnt     Score      Error  Units
Redis4CatsHashesBenchmark.hashAllReader                  thrpt    5   241.587 ±   26.180  ops/s
Redis4CatsHashesBenchmark.hashFieldValueReader           thrpt    5   244.069 ±   17.434  ops/s
Redis4CatsKeysBenchmark.keyExistsReader                  thrpt    5   243.712 ±   30.641  ops/s
Redis4CatsKeysBenchmark.keyPttlReader                    thrpt    5   243.824 ±   25.497  ops/s
Redis4CatsListsBenchmark.listByIndexReader               thrpt    5   243.921 ±   26.118  ops/s
Redis4CatsListsBenchmark.listLengthReader                thrpt    5   244.981 ±   19.299  ops/s
Redis4CatsListsBenchmark.listRangeReader                 thrpt    5   240.766 ±   41.103  ops/s
Redis4CatsSetsBenchmark.setCardReader                    thrpt    5   243.207 ±   34.426  ops/s
Redis4CatsSetsBenchmark.setDiffWriter                    thrpt    5   243.737 ±   40.443  ops/s
Redis4CatsSetsBenchmark.setMembersReader                 thrpt    5   245.037 ±   26.443  ops/s
Redis4CatsSortedSetsBenchmark.sortedSetCardReader        thrpt    5   244.748 ±   15.027  ops/s
Redis4CatsSortedSetsBenchmark.sortedSetCountReader       thrpt    5   244.537 ±   20.392  ops/s
Redis4CatsSortedSetsBenchmark.sortedSetRangeReader       thrpt    5   243.733 ±   29.076  ops/s
Redis4CatsStringsBenchmark.stringAppender                thrpt    5   243.714 ±   34.937  ops/s
Redis4CatsStringsBenchmark.stringReader                  thrpt    5   246.181 ±   20.508  ops/s
Redis4CatsWriterBenchmark.hashWriter                     thrpt    5   240.144 ±   31.877  ops/s
Redis4CatsWriterBenchmark.listWriter                     thrpt    5   239.150 ±   36.041  ops/s
Redis4CatsWriterBenchmark.setWriter                      thrpt    5   239.792 ±   41.365  ops/s
Redis4CatsWriterBenchmark.sortedSetWriter                thrpt    5   241.359 ±   29.813  ops/s
Redis4CatsWriterBenchmark.stringWriter                   thrpt    5   238.584 ±   38.784  ops/s
```

*laserdisc*
```
Benchmark                                                  Mode  Cnt     Score      Error  Units
RedisLaserdiscHashesBenchmark.hashAllReader               thrpt    5   206.600 ±   16.652  ops/s
RedisLaserdiscHashesBenchmark.hashFieldValueReader        thrpt    5   202.798 ±   19.260  ops/s
RedisLaserdiscKeysBenchmark.keyExistsReader               thrpt    5   204.249 ±   29.891  ops/s
RedisLaserdiscKeysBenchmark.keyPttlReader                 thrpt    5   207.218 ±   27.851  ops/s
RedisLaserdiscListsBenchmark.listByIndexReader            thrpt    5   201.452 ±   25.822  ops/s
RedisLaserdiscListsBenchmark.listLengthReader             thrpt    5   209.735 ±   13.533  ops/s
RedisLaserdiscListsBenchmark.listRangeReader              thrpt    5   208.484 ±   15.703  ops/s
RedisLaserdiscSetsBenchmark.setCardReader                 thrpt    5   206.543 ±   20.895  ops/s
RedisLaserdiscSetsBenchmark.setDiffWriter                 thrpt    5   204.151 ±   33.102  ops/s
RedisLaserdiscSetsBenchmark.setMembersReader              thrpt    5   210.666 ±   12.511  ops/s
RedisLaserdiscSortedSetsBenchmark.sortedSetCardReader     thrpt    5   210.381 ±   16.151  ops/s
RedisLaserdiscSortedSetsBenchmark.sortedSetCountReader    thrpt    5   210.723 ±   19.308  ops/s
RedisLaserdiscSortedSetsBenchmark.sortedSetRangeReader    thrpt    5   208.416 ±    6.888  ops/s
RedisLaserdiscStringsBenchmark.stringAppender             thrpt    5   209.898 ±   17.705  ops/s
RedisLaserdiscStringsBenchmark.stringReader               thrpt    5   210.584 ±   19.734  ops/s
RedisLaserdiscWriterBenchmark.hashWriter                  thrpt    5   194.519 ±   76.122  ops/s
RedisLaserdiscWriterBenchmark.listWriter                  thrpt    5   193.732 ±   61.924  ops/s
RedisLaserdiscWriterBenchmark.setWriter                   thrpt    5   191.630 ±   65.897  ops/s
RedisLaserdiscWriterBenchmark.sortedSetWriter             thrpt    5   195.070 ±   58.346  ops/s
RedisLaserdiscWriterBenchmark.stringWriter                thrpt    5   196.036 ±   61.414  ops/s
```

*redicolous*
```
Benchmark                                                 Mode  Cnt     Score      Error  Units
RedisRedicolousHashesBenchmark.hashAllReader             thrpt    5   191.744 ±   11.793  ops/s
RedisRedicolousHashesBenchmark.hashFieldValueReader      thrpt    5   188.838 ±   14.652  ops/s
RedisRedicolousKeysBenchmark.keyExistsReader             thrpt    5   180.281 ±   33.930  ops/s
RedisRedicolousKeysBenchmark.keyPttlReader               thrpt    5   177.798 ±   25.956  ops/s
RedisRedicolousListsBenchmark.listByIndexReader          thrpt    5   178.907 ±   16.155  ops/s
RedisRedicolousListsBenchmark.listLengthReader           thrpt    5   185.376 ±   17.431  ops/s
RedisRedicolousListsBenchmark.listRangeReader            thrpt    5   190.611 ±   18.494  ops/s
RedisRedicolousSetsBenchmark.setCardReader               thrpt    5   195.075 ±   20.708  ops/s
RedisRedicolousSetsBenchmark.setDiffWriter               thrpt    5   197.998 ±    7.892  ops/s
RedisRedicolousSetsBenchmark.setMembersReader            thrpt    5   192.814 ±   12.913  ops/s
RedisRedicolousSortedSetsBenchmark.sortedSetCardReader   thrpt    5   194.387 ±   17.450  ops/s
RedisRedicolousSortedSetsBenchmark.sortedSetCountReader  thrpt    5   190.964 ±   15.650  ops/s
RedisRedicolousSortedSetsBenchmark.sortedSetRangeReader  thrpt    5   191.128 ±   18.725  ops/s
RedisRedicolousStringsBenchmark.stringAppender           thrpt    5   192.123 ±   12.136  ops/s
RedisRedicolousStringsBenchmark.stringReader             thrpt    5   191.871 ±   19.730  ops/s
RedisRedicolousWriterBenchmark.hashWriter                thrpt    5   178.010 ±   56.275  ops/s
RedisRedicolousWriterBenchmark.listWriter                thrpt    5   178.274 ±   57.184  ops/s
RedisRedicolousWriterBenchmark.setWriter                 thrpt    5   175.585 ±   49.202  ops/s
RedisRedicolousWriterBenchmark.sortedSetWriter           thrpt    5   179.585 ±   57.274  ops/s
RedisRedicolousWriterBenchmark.stringWriter              thrpt    5   177.122 ±   55.343  ops/s
```
```
sbt 'benchmarks/jmh:run -i 5 -wi 1 -f1 -t1 monix.connect.benchmarks.redis.*Benchmark.*'
```