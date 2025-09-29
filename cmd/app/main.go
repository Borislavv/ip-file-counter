package main

import (
	"flag"
	"fmt"
	"github.com/Borislavv/ip-file-counter/internal/read"
	"os"
	"path/filepath"
	"time"
)

var (
	flagShards  = flag.Int("shards", 256, "number of shards (default: min(GOMAXPROCS*4,64))")
	flagReaders = flag.Int("readers", 48, "number of parallel readers (default: min(GOMAXPROCS,8))")
	flagBufMB   = flag.Int("bufMB", 32, "per-reader block size in Mb")
	flagProbeKB = flag.Int("probeKB", 4, "segment align probe window in Kb")
)

func main() {
	var from = time.Now()

	flag.Parse()
	if flag.NArg() < 1 {
		_, _ = fmt.Fprintf(os.Stderr, "usage: %s <path-to-file>\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}
	path := flag.Arg(0)

	total, err := read.UniqueIPv4Count(path, *flagShards, *flagReaders, *flagBufMB, *flagProbeKB)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "ERR:", err)
		os.Exit(2)
	}
	fmt.Printf("Unique IPv4 Count: %d, elapsed: %s.\n", total, time.Since(from).String())
}
