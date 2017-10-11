Hardware
--------

Cores: 6 
Memory: 24 Gig
Drive: MKNSSDTR480GB


gopgmq Go Benchmark Run 30s
```````````````````````````

cmd::

  go test -bench . -benchtime=30s

output::

  BenchmarkPublishConsume1-12                20000           2691070 ns/op           71787 B/op        152 allocs/op
  BenchmarkPublishConsume10-12               20000           2888003 ns/op           79984 B/op        319 allocs/op
  BenchmarkPublishConsume100-12              10000           5146879 ns/op          162020 B/op       1944 allocs/op
  BenchmarkPublishConsume1000-12              2000          28623759 ns/op          988142 B/op      18156 allocs/op
  BenchmarkPublishConsume10000-12              200         373655489 ns/op         9454956 B/op     180273 allocs/op

========== ==================
# Messages Message Per Second
========== ==================
1          371
10         3,462
100				 19,429
1,000			 34,936
10,000 		 26,762
========== ==================

liteq Go Benchmark Run 30s
```````````````````````````
Sqlite:

cmd::

  go test -bench . -benchtime=30s

output::

  goos: freebsd                                  
  goarch: amd64                                  
  pkg: github.com/lateefj/gq/liteq               
  BenchmarkPublishConsume1-12                 5000           8890314 ns/op           0.01 MB/s        5614 B/op        157 allocs/op
  BenchmarkPublishConsume10-12               10000           4921144 ns/op           0.02 MB/s       12444 B/op        347 allocs/op
  BenchmarkPublishConsume100-12              10000           7205610 ns/op           0.01 MB/s       79031 B/op       2153 allocs/op
  BenchmarkPublishConsume1000-12              2000          21145583 ns/op           0.00 MB/s      745178 B/op      20162 allocs/op
  BenchmarkPublishConsume10000-12              300         158010344 ns/op           0.00 MB/s     8013517 B/op     200265 allocs/op


