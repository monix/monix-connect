## Redis Benchmark

### 2020/10

*1Fork 1Thread*
```
Benchmark                                       Mode  Cnt     Score     Error  Units
RedisHashesBenchmark.hashAllReader             thrpt    5  1680.818 ± 253.323  ops/s
RedisHashesBenchmark.hashFieldValueReader      thrpt    5  1728.129 ± 157.907  ops/s
RedisKeysBenchmark.keyExistsReader             thrpt    5  1756.169 ± 262.615  ops/s
RedisKeysBenchmark.keyPttlReader               thrpt    5  1775.751 ± 241.195  ops/s
RedisListsBenchmark.listByIndexReader          thrpt    5  1771.289 ± 220.066  ops/s
RedisListsBenchmark.listLengthReader           thrpt    5  1766.981 ± 177.582  ops/s
RedisListsBenchmark.listRangeReader            thrpt    5  1765.229 ± 311.742  ops/s
RedisSetsBenchmark.setDiffWriter               thrpt    5  1749.234 ± 171.951  ops/s
RedisSetsBenchmark.setMembersReader            thrpt    5  1768.085 ± 198.603  ops/s
RedisSetsBenchmark.setCardReader               thrpt    5  1718.356 ±  80.820  ops/s
RedisSortedSetsBenchmark.sortedSetCardReader   thrpt    5  1743.183 ± 229.873  ops/s
RedisSortedSetsBenchmark.sortedSetCountReader  thrpt    5  1790.768 ± 281.572  ops/s
RedisSortedSetsBenchmark.sortedSetRangeReader  thrpt    5  1836.357 ± 150.275  ops/s
RedisStringsBenchmark.stringAppender           thrpt    5  1753.049 ± 226.702  ops/s
RedisStringsBenchmark.stringReader             thrpt    5  1767.115 ± 127.024  ops/s
RedisWriterBenchmark.hashWriter                thrpt    5  1714.006 ±  28.224  ops/s
RedisWriterBenchmark.listWriter                thrpt    5  1799.224 ± 235.473  ops/s
RedisWriterBenchmark.setWriter                 thrpt    5  1805.233 ± 281.053  ops/s
RedisWriterBenchmark.sortedSetWriter           thrpt    5  1740.674 ± 196.590  ops/s
RedisWriterBenchmark.stringWriter              thrpt    5  1822.662 ± 149.060  ops/s
```

```
sbt 'benchmarks/jmh:run -i 2 -wi 1 -f1 -t1 monix.connect.benchmarks.redis.*Benchmark.*'
```