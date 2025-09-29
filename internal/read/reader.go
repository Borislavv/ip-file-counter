package read

import (
	"bytes"
	"io"
	"math/bits"
	"os"
	"runtime"
	"sync"
)

func UniqueIPv4Count(path string, shards, readers, bufMb, probeKb int) (uint64, error) {
	S := shards
	if S <= 0 {
		S = runtime.GOMAXPROCS(0) * 4
		if S > 64 {
			S = 64
		}
		if S < 1 {
			S = 1
		}
	}
	R := readers
	if R <= 0 {
		R = runtime.GOMAXPROCS(0)
		if R > 8 {
			R = 8
		}
		if R < 1 {
			R = 1
		}
	}
	readBuf := bufMb * (1 << 20)
	probeThresholdKb := int64(probeKb << 10)

	// Bitsets per shard: exact 2^32 coverage
	const totalBits = uint64(1) << 32
	bitsPerShard := (totalBits + uint64(S) - 1) / uint64(S)
	wordsPerShard := int((bitsPerShard + 63) / 64)

	shardBits := make([][]uint64, S)
	in := make([]chan []uint32, S)
	for i := 0; i < S; i++ {
		shardBits[i] = make([]uint64, wordsPerShard)
		in[i] = make(chan []uint32, 64) // deeper buffer to reduce reader stalls
	}

	// Aggregators: single-owner bitset, no locks.
	var aggWG sync.WaitGroup
	counts := make([]uint64, S)
	aggWG.Add(S)
	for id := 0; id < S; id++ {
		id := id
		go func() {
			defer aggWG.Done()
			bs := shardBits[id]
			for batch := range in[id] {
				for _, ip := range batch {
					off := uint64(ip) / uint64(S)
					w := off >> 6
					b := off & 63
					bs[w] |= 1 << b
				}
				putBatch(batch)
			}
			var c uint64
			for _, w := range bs {
				c += uint64(bits.OnesCount64(w))
			}
			counts[id] = c
		}()
	}

	// Single shared file handle (ReadAt is concurrency-safe).
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()

	// Prepare R segments (independent from S shards) + align to '\n' (left-only) and stitch.
	segs := split(size, R)
	if err := alignSegments(f, segs, probeThresholdKb); err != nil {
		return 0, err
	}

	// Parallel readers per segment.
	var rdWG sync.WaitGroup
	rdWG.Add(len(segs))
	for i := range segs {
		seg := segs[i]
		isLast := (seg.hi == size) // real last by file end
		go func(lo, hi int64, last bool) {
			defer rdWG.Done()
			readSegmentReadAt(f, lo, hi, last, in, readBuf)
		}(seg.lo, seg.hi, isLast)
	}
	rdWG.Wait()
	for i := 0; i < S; i++ {
		close(in[i])
	}
	aggWG.Wait()

	var total uint64
	for _, c := range counts {
		total += c
	}
	return total, nil
}

type segment struct{ lo, hi int64 }

func split(size int64, parts int) []segment {
	if parts <= 1 || size <= 0 {
		return []segment{{0, size}}
	}
	out := make([]segment, parts)
	chunk := size / int64(parts)
	lo := int64(0)
	for i := 0; i < parts; i++ {
		hi := lo + chunk
		if i == parts-1 {
			hi = size
		}
		out[i] = segment{lo, hi}
		lo = hi
	}
	return out
}

// alignSegments does left-only alignment on i>0 within PROBE window,
// then stitches segments so that seg[i].hi == seg[i+1].lo and the last seg.hi == file end.
// This guarantees no gaps/overlaps and no split lines across segments.
func alignSegments(f *os.File, segs []segment, probe int64) error {
	if probe < 1 || len(segs) == 0 {
		return nil
	}
	orig := make([]segment, len(segs))
	copy(orig, segs)

	tmp := make([]byte, probe)

	// Left-align: for i>0 move lo to just after the next '\n' within window; if none, empty the segment.
	for i := 1; i < len(segs); i++ {
		lo, hi := orig[i].lo, orig[i].hi
		if lo >= hi {
			segs[i].lo = lo
			continue
		}
		win := hi - lo
		if win > probe {
			win = probe
		}
		n, _ := f.ReadAt(tmp[:win], lo)
		if k := bytes.IndexByte(tmp[:n], '\n'); k >= 0 {
			segs[i].lo = lo + int64(k+1)
		} else {
			// No newline in window -> make this segment empty to avoid cutting a line.
			segs[i].lo = hi
		}
	}
	// First segment starts at original start.
	segs[0].lo = orig[0].lo

	// Stitch: previous.hi = next.lo; last.hi = file end.
	for i := 0; i+1 < len(segs); i++ {
		segs[i].hi = segs[i+1].lo
		if segs[i].hi < segs[i].lo {
			segs[i].hi = segs[i].lo
		}
	}
	last := len(segs) - 1
	segs[last].hi = orig[last].hi
	if segs[last].hi < segs[last].lo {
		segs[last].hi = segs[last].lo
	}
	return nil
}

func readSegmentReadAt(f *os.File, lo, hi int64, isLast bool, outs []chan []uint32, bufSize int) {
	if hi <= lo {
		return
	}
	buf := make([]byte, bufSize)
	var local = make([][]uint32, len(outs))

	// carry for boundary line (IPv4 fits ≤ 16 bytes incl. CR)
	var carry [32]byte
	carryLen := 0

	flushLocal := func() {
		for id, b := range local {
			if len(b) > 0 {
				outs[id] <- b
				local[id] = nil
			}
		}
	}

	pos := lo
	for pos < hi {
		want := buf
		if rem := hi - pos; int64(len(want)) > rem {
			want = want[:rem]
		}
		n, er := f.ReadAt(want, pos)
		if n == 0 && (er == io.EOF || er == nil) {
			break
		}
		chunk := want[:n]
		pos += int64(len(chunk))

		i := 0

		// complete carried line if present
		if carryLen > 0 {
			if k := bytes.IndexByte(chunk, '\n'); k >= 0 {
				need := carryLen + k
				if need > len(carry) {
					need = len(carry) // safety (shouldn't hit with IPv4 lines)
				}
				copy(carry[carryLen:], chunk[:need-carryLen])
				line := carry[:need]
				// strip CR
				if need > 0 && line[need-1] == '\r' {
					line = line[:need-1]
				}
				if ip, ok := parseIPv4(line); ok {
					sid := int(uint32(ip) % uint32(len(outs)))
					if local[sid] == nil {
						local[sid] = getBatch()
					}
					local[sid] = append(local[sid], ip)
					if len(local[sid]) >= batchSize {
						outs[sid] <- local[sid]
						local[sid] = nil
					}
				}
				carryLen = 0
				i = k + 1
			} else {
				// no newline in this chunk; extend carry safely
				avail := len(carry) - carryLen
				if avail > 0 {
					to := len(chunk)
					if to > avail {
						to = avail
					}
					copy(carry[carryLen:], chunk[:to])
					carryLen += to
				}
				continue
			}
		}

		// fast path: scan lines within chunk
		for {
			j := bytes.IndexByte(chunk[i:], '\n')
			if j < 0 {
				// save tail into carry (≤16 bytes)
				if i < len(chunk) {
					tail := chunk[i:]
					if len(tail) > 0 {
						if len(tail) > len(carry) {
							tail = tail[len(tail)-len(carry):] // safety
						}
						copy(carry[:], tail)
						carryLen = len(tail)
					}
				}
				break
			}
			end := i + j
			line := chunk[i:end]
			// strip CR
			if ln := len(line); ln > 0 && line[ln-1] == '\r' {
				line = line[:ln-1]
			}
			if ip, ok := parseIPv4(line); ok {
				sid := int(uint32(ip) % uint32(len(outs)))
				if local[sid] == nil {
					local[sid] = getBatch()
				}
				local[sid] = append(local[sid], ip)
				if len(local[sid]) >= batchSize {
					outs[sid] <- local[sid]
					local[sid] = nil
				}
			}
			i = end + 1
		}

		if er == io.EOF {
			break
		}
	}

	// last segment may end without '\n'
	if isLast && carryLen > 0 {
		line := carry[:carryLen]
		if ln := len(line); ln > 0 && line[ln-1] == '\r' {
			line = line[:ln-1]
		}
		if ip, ok := parseIPv4(line); ok {
			sid := int(uint32(ip) % uint32(len(outs)))
			b := getBatch()
			b = append(b, ip)
			outs[sid] <- b
		}
	}

	flushLocal()
}

const batchSize = 32768

var batchPool = sync.Pool{
	New: func() any { return make([]uint32, 0, batchSize) },
}

func getBatch() []uint32  { return batchPool.Get().([]uint32)[:0] }
func putBatch(b []uint32) { batchPool.Put(b[:0]) }

func parseIPv4(b []byte) (uint32, bool) {
	var a0, a1, a2, a3 uint32
	var i, n int
	a0, n = dec3(b, 0)
	if n == 0 || n >= len(b) || b[n] != '.' || a0 > 255 {
		return 0, false
	}
	i = n + 1
	a1, n = dec3(b, i)
	if n == i || n >= len(b) || b[n] != '.' || a1 > 255 {
		return 0, false
	}
	i = n + 1
	a2, n = dec3(b, i)
	if n == i || n >= len(b) || b[n] != '.' || a2 > 255 {
		return 0, false
	}
	i = n + 1
	a3, n = dec3(b, i)
	// Allow optional trailing '\r' without '\n'.
	if a3 > 255 {
		return 0, false
	}
	switch {
	case n == len(b):
	case n+1 == len(b) && b[n] == '\r':
	default:
		return 0, false
	}
	return (a0 << 24) | (a1 << 16) | (a2 << 8) | a3, true
}

// dec3 parses up to 3 ASCII digits starting at i, returns (value, newIndex).
func dec3(b []byte, i int) (uint32, int) {
	n := len(b)
	if i >= n || b[i] < '0' || b[i] > '9' {
		return 0, i
	}
	v := uint32(b[i] - '0')
	i++
	if i < n && b[i] >= '0' && b[i] <= '9' {
		v = v*10 + uint32(b[i]-'0')
		i++
		if i < n && b[i] >= '0' && b[i] <= '9' {
			v = v*10 + uint32(b[i]-'0')
			i++
		}
	}
	return v, i
}
