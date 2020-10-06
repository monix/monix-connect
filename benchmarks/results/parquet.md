

## Parquet Benchmark

### 2020/10
1Fork 1Thread
[info] Benchmark                           Mode  Cnt     Score     Error  Units
[info] ParquetReaderBenchmark.fromTask    thrpt    5  2191.529 ± 493.092  ops/s
[info] ParquetReaderBenchmark.unsafe      thrpt    5  2708.631 ± 508.417  ops/s
[info] ParquetWriterBenchmark.fromCoeval  thrpt    5    98.009 ±  17.250  ops/s
[info] ParquetWriterBenchmark.fromTask    thrpt    5    97.417 ±  15.160  ops/s
[info] ParquetWriterBenchmark.unsafe      thrpt    5   100.652 ±   8.674  ops/s

2Forks 3Threads

[info] Benchmark                           Mode  Cnt     Score      Error  Units
*10 records per file*
[info] ParquetReaderBenchmark.fromTask    thrpt   10  6772.867 ± 1562.102  ops/s
[info] ParquetReaderBenchmark.unsafe      thrpt   10  7433.957 ±  129.478  ops/s
[info] ParquetWriterBenchmark.fromCoeval  thrpt    8   233.075 ±   54.298  ops/s
[info] ParquetWriterBenchmark.fromTask    thrpt    8   241.534 ±   17.257  ops/s
[info] ParquetWriterBenchmark.unsafe      thrpt    8   244.405 ±   28.380  ops/s

*250 records per file*
[info] ParquetReaderBenchmark.fromTask    thrpt   10  3789.921 ± 54.236  ops/s
[info] ParquetReaderBenchmark.unsafe      thrpt   10  4079.695 ± 95.907  ops/s
[info] ParquetWriterBenchmark.fromCoeval  thrpt    8   248.694 ±  8.811  ops/s
[info] ParquetWriterBenchmark.fromTask    thrpt    8   239.736 ±  9.499  ops/s
[info] ParquetWriterBenchmark.unsafe      thrpt    8   176.922 ± 69.186  ops/s
```
sbt 'benchmarks/jmh:run -i 5 -wi 1 -f1 -t1 monix.connect.benchmarks.parquet.*Benchmark.*'
```