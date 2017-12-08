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

  goos: freebsd                                  
  goarch: amd64                                  
  pkg: github.com/lateefj/gq/pgmq                
  BenchmarkPublishConsume1-12                20000           2871147 ns/op           0.03 MB/s       72058 B/op        153 allocs/op
  BenchmarkPublishConsume10-12               10000           3217504 ns/op           0.03 MB/s       79476 B/op        289 allocs/op
  BenchmarkPublishConsume100-12               5000           7948995 ns/op           0.01 MB/s      152382 B/op       1557 allocs/op
  BenchmarkPublishConsume1000-12              1000          37772015 ns/op           0.00 MB/s      883921 B/op      14172 allocs/op
  BenchmarkPublishConsume10000-12              100         325619290 ns/op           0.00 MB/s     8582439 B/op     140404 allocs/op

============ =================== ===================
# Batch Size Produced Per Second Consumed Per Second
============ =================== ===================
10           15,599              2,948
100          50,122              4,940
1,000        73,995              6,509 
10,000       38,125              8,382
============ =================== ===================

liteq Go Benchmark Run 30s
```````````````````````````
Sqlite:

cmd::

  go test -bench . -benchtime=30s

output::

  goos: freebsd                                  
  goarch: amd64                                  
  pkg: github.com/lateefj/gq/liteq               
  BenchmarkPublishConsume1-12                 5000           8870224 ns/op           0.01 MB/s        5594 B/op        157 allocs/op
  BenchmarkPublishConsume10-12               10000           5155761 ns/op           0.02 MB/s       12449 B/op        347 allocs/op
  BenchmarkPublishConsume100-12               5000           6674932 ns/op           0.01 MB/s       79029 B/op       2153 allocs/op
  BenchmarkPublishConsume1000-12              2000          22170653 ns/op           0.00 MB/s      744272 B/op      20162 allocs/op
  BenchmarkPublishConsume10000-12              300         163540961 ns/op           0.00 MB/s     8012253 B/op     200265 allocs/op

============ =================== ===================
# Batch Size Produced Per Second Consumed Per Second
============ =================== ===================
10           2,143               1,290
100          11,847              7,066
1,000        21,598              12,310
10,000       37,417              10,116
============ =================== ===================
