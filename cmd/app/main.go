package main

import (
	"flag"
	"fmt"
	"github.com/Borislavv/ip-file-counter/internal/read"
	"os"
	"path/filepath"
)

var (
	flagShards  = flag.Int("shards", 0, "number of shards (default: min(GOMAXPROCS*4,64))")
	flagReaders = flag.Int("readers", 0, "number of parallel readers (default: min(GOMAXPROCS,8))")
	flagBufMB   = flag.Int("bufMB", 64, "per-reader block size in Mb")
	flagProbeKB = flag.Int("probeKB", 64, "segment align probe window in Kb")
)

func main() {
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
	fmt.Println(total)
}
