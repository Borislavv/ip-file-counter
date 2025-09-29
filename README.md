# ip-file-counter

Count unique IPv4 addresses in very large text files (tens/hundreds of GB). One line = one IPv4.

## Spoiler - results

I have already solved 1 Billion Rows Challenge a few years earlier. [Go to 1BRC repo](https://github.com/Borislavv/go-1brc).


<img width="576" height="108" alt="image" src="https://github.com/user-attachments/assets/804fcfe4-1e66-467c-b8e9-92101fcc2228" />

## Build
```bash
# counter
go build -o ip-uniq ./cmd/app

# mock generator (creates big files with duplicates)
go build -o ip-gen  ./cmd/gen
```

## Usage
### Count unique IPv4s
```bash
./ip-uniq -shards 256 -readers 48 -bufMB 32 -probeKB 4 /path/to/ips.txt
# output: "Unique IPv4 Count: <N>, elapsed: <dur>."
```
Flags:
- `-shards` — number of aggregation shards (default `256`)
- `-readers` — parallel readers (default `48`)
- `-bufMB` — per-reader block size in MiB (default `32`)
- `-probeKB` — alignment probe window in KiB (default `4`)

### Generate a mock file
```bash
# size is bytes (default: 1 GiB)
./ip-gen -size 1073741824
# logs the generated file path; name encodes human size, total lines, unique lines
```

## Tests
```bash
go test -v ./cmd/app
```

## Benchmarks (measure only the counting path)
Provide a real file via env var, then run package-local benchmarks:
```bash
export IP_BENCH_FILE=/absolute/path/to/ips.txt
go test -run=^$ -bench . -benchmem ./cmd/app
```

## Profiling (single package)
```bash
# build a test binary
go test -c -o app.test ./cmd/app

# run one benchmark with profiles
IP_BENCH_FILE=/path/to/ips.txt ./app.test -test.run=^$ -test.bench ^BenchmarkCount_Throughput$ -test.benchmem   -test.cpuprofile=cpu.out -test.memprofile=mem.out

# explore
go tool pprof -http=:8080 ./app.test cpu.out
go tool pprof -http=:8081 ./app.test mem.out
```
