package codec

// ParseIPv4 returns ip in network order: A.B.C.D => A<<24|B<<16|C<<8|D
func ParseIPv4(b []byte) (uint32, bool) {
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
	if n != len(b) || a3 > 255 {
		return 0, false
	}
	return (a0 << 24) | (a1 << 16) | (a2 << 8) | a3, true
}
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
