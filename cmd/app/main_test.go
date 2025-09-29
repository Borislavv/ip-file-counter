package main

import (
	"bufio"
	"github.com/Borislavv/ip-file-counter/internal/codec"
	"github.com/Borislavv/ip-file-counter/internal/read"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func writeTempFile(t *testing.T, name string, lines []string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	for i, s := range lines {
		if _, err := w.WriteString(s); err != nil {
			t.Fatalf("write line %d: %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return path
}

// small, strict reference counter (strconv-based); for correctness checks on small files
func refUniqueIPv4Count(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	seen := make(map[uint32]struct{}, 1024)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if ip, ok := refParseIPv4(line); ok {
			seen[ip] = struct{}{}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return len(seen)
}

func refParseIPv4(s string) (uint32, bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return 0, false
	}
	var a [4]uint64
	for i := 0; i < 4; i++ {
		if parts[i] == "" {
			return 0, false
		}
		x, err := strconv.ParseUint(parts[i], 10, 32)
		if err != nil || x > 255 {
			return 0, false
		}
		a[i] = x
	}
	ip := (uint32(a[0]) << 24) | (uint32(a[1]) << 16) | (uint32(a[2]) << 8) | uint32(a[3])
	return ip, true
}

func ipToString(a, b, c, d int, leadingZeros bool) string {
	if leadingZeros {
		return pad3(a) + "." + pad3(b) + "." + pad3(c) + "." + pad3(d)
	}
	return strconv.Itoa(a) + "." + strconv.Itoa(b) + "." + strconv.Itoa(c) + "." + strconv.Itoa(d)
}

func pad3(x int) string {
	if x < 0 {
		x = 0
	} else if x > 255 {
		x = 255
	}
	switch {
	case x < 10:
		return "00" + strconv.Itoa(x)
	case x < 100:
		return "0" + strconv.Itoa(x)
	default:
		return strconv.Itoa(x)
	}
}

func TestParseIPv4_Primitives(t *testing.T) {
	type C struct {
		in string
		ok bool
		ip uint32
	}
	cases := []C{
		{"0.0.0.0", true, 0},
		{"1.2.3.4", true, (1 << 24) | (2 << 16) | (3 << 8) | 4},
		{"255.255.255.255", true, 0xFFFFFFFF},
		{"10.0.0.1\r", true, (10 << 24) | (0 << 16) | (0 << 8) | 1},
		{"01.02.003.004", true, (1 << 24) | (2 << 16) | (3 << 8) | 4},
		{"999.0.0.1", false, 0},
		{"1.2.3", false, 0},
		{"1.2.3.4.5", false, 0},
		{"a.b.c.d", false, 0},
		{"", false, 0},
		{"1..3.4", false, 0},
		{"256.0.0.1", false, 0},
	}
	for _, c := range cases {
		ip, ok := codec.ParseIPv4([]byte(c.in))
		if ok != c.ok {
			t.Fatalf("parseIPv4(%q) ok=%v, want %v", c.in, ok, c.ok)
		}
		if ok && ip != c.ip {
			t.Fatalf("parseIPv4(%q) ip=%08x, want %08x", c.in, ip, c.ip)
		}
	}
}

func TestParseIPv4_NoAllocs(t *testing.T) {
	samples := [][]byte{
		[]byte("1.2.3.4"),
		[]byte("10.0.0.1"),
		[]byte("255.255.255.255"),
		[]byte("123.045.006.007"),
		[]byte("192.168.0.1\r"),
	}
	for _, s := range samples {
		allocs := testing.AllocsPerRun(1000, func() {
			codec.ParseIPv4(s)
		})
		if allocs != 0 {
			t.Fatalf("parseIPv4(%q) allocs=%v, want 0", string(s), allocs)
		}
	}
}

func TestUniqueIPv4_SimpleLF(t *testing.T) {
	lines := []string{
		"0.0.0.0\n",
		"1.2.3.4\n",
		"255.255.255.255\n",
		"1.2.3.4\n",
		"10.0.0.1\n",
		"192.168.0.1\n",
		"999.0.0.1\n",
		"a.b.c.d\n",
		"\n",
	}
	path := writeTempFile(t, "simple_lf.txt", lines)

	want := refUniqueIPv4Count(t, path)
	got, err := read.UniqueIPv4Count(path, min(runtime.GOMAXPROCS(0)*2, 8), min(runtime.GOMAXPROCS(0), 2), 64, 64)
	if err != nil {
		t.Fatalf("uniqueIPv4Count err: %v", err)
	}
	if int(got) != want {
		t.Fatalf("count=%d, want %d", got, want)
	}
}

func TestUniqueIPv4_CRLF_And_NoNewlineTail(t *testing.T) {
	lines := []string{
		"1.1.1.1\r\n",
		"2.2.2.2\r\n",
		"3.3.3.3",
	}
	path := writeTempFile(t, "crlf_tail.txt", lines)

	want := refUniqueIPv4Count(t, path)
	got, err := read.UniqueIPv4Count(path, min(runtime.GOMAXPROCS(0)*2, 8), min(runtime.GOMAXPROCS(0), 3), 64, 32)
	if err != nil {
		t.Fatalf("uniqueIPv4Count err: %v", err)
	}
	if int(got) != want {
		t.Fatalf("count=%d, want %d", got, want)
	}
}

func TestUniqueIPv4_SmallMixedReadersShards(t *testing.T) {
	base := []string{
		"10.0.0.1\n",
		"10.0.0.2\r\n",
		"10.0.0.3\n",
		"10.0.0.4\r\n",
		"172.16.0.1\n",
		"172.16.0.1\n",
		"192.168.1.1\r\n",
		"192.168.1.2\n",
		"255.255.255.255\n",
		"0.0.0.0\r\n",
	}
	lines := append([]string{}, base...)
	lines = append(lines, "10.0.0.2\n", "a.b.c.d\n", "1.2.3\n")

	path := writeTempFile(t, "small_mixed.txt", lines)
	want := refUniqueIPv4Count(t, path)

	cfgs := [][2]int{
		{1, 1},
		{2, 4},
		{4, 8},
	}
	for _, cfg := range cfgs {
		got, err := read.UniqueIPv4Count(path, cfg[1], cfg[0], 64, 64)
		if err != nil {
			t.Fatalf("uniqueIPv4Count err (R=%d,S=%d): %v", cfg[0], cfg[1], err)
		}
		if int(got) != want {
			t.Fatalf("R=%d S=%d: count=%d, want %d", cfg[0], cfg[1], got, want)
		}
	}
}

func TestBoundary_ProbeTiny_NoLoss(t *testing.T) {
	const n = 5000
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		a := (i >> 24) & 255
		b := (i >> 16) & 255
		c := (i >> 8) & 255
		d := i & 255
		lines = append(lines, ipToString(a, b, c, d, false)+"\n")
	}
	for i := 50; i < n; i += 777 {
		lines[i] = strings.TrimSuffix(lines[i], "\n") + "\r\n"
	}
	for i := 100; i < n; i += 997 {
		lines[i] = "300.400.500.600\n"
	}

	path := writeTempFile(t, "probe_tiny.txt", lines)
	want := refUniqueIPv4Count(t, path)

	got, err := read.UniqueIPv4Count(path, 32, 3, 8, 1)
	if err != nil {
		t.Fatalf("uniqueIPv4Count err: %v", err)
	}
	if int(got) != want {
		t.Fatalf("count=%d, want %d", got, want)
	}
}

func TestRandomCorpus_Medium_NoLoss(t *testing.T) {
	r := rand.New(rand.NewSource(1)) // deterministic
	const n = 20000
	lines := make([]string, 0, n+100)

	for i := 0; i < n; i++ {
		a := r.Intn(256)
		b := r.Intn(256)
		c := r.Intn(256)
		d := r.Intn(256)
		withZeros := r.Intn(2) == 0
		end := "\n"
		if r.Intn(3) == 0 {
			end = "\r\n"
		}
		lines = append(lines, ipToString(a, b, c, d, withZeros)+end)
		if r.Intn(5) == 0 {
			lines = append(lines, ipToString(a, b, c, d, withZeros)+end)
		}
	}
	invalids := []string{"", "a.b.c.d\n", "1.2.3\n", "999.1.2.3\n", "1..2.3\n"}
	lines = append(lines, invalids...)
	if r.Intn(2) == 0 {
		if strings.HasSuffix(lines[len(lines)-1], "\n") {
			lines[len(lines)-1] = strings.TrimSuffix(lines[len(lines)-1], "\n")
		}
	}

	path := writeTempFile(t, "random_medium.txt", lines)
	want := refUniqueIPv4Count(t, path)

	cfgs := []struct{ R, S, buf, probe int }{
		{1, 1, 64, 64},
		{min(runtime.GOMAXPROCS(0), 4), 8, 64, 64},
	}
	for _, cfg := range cfgs {
		got, err := read.UniqueIPv4Count(path, cfg.S, cfg.R, cfg.S, cfg.probe)
		if err != nil {
			t.Fatalf("uniqueIPv4Count err (R=%d S=%d): %v", cfg.R, cfg.S, err)
		}
		if int(got) != want {
			t.Fatalf("R=%d S=%d: count=%d, want %d", cfg.R, cfg.S, got, want)
		}
	}
}
