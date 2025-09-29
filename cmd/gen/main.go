package main

import (
	"flag"
	"log"

	"github.com/Borislavv/ip-file-counter/internal/gen"
)

var size = flag.Int64("size", 1<<30, "target file size in bytes (default 1Gb)")

func init() {
	flag.Parse()
}

func main() {
	if *size <= 0 {
		log.Fatalf("invalid weight flag: must be > 0 (bytes)")
	}

	if fp, err := gen.IPsFile(*size); err != nil {
		log.Fatalf("could not generate IP file: %v", err)
	} else {
		log.Printf("generated IP file: %s (target %d bytes)\n", fp, *size)
	}
}
