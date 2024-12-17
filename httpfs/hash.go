package httpfs

// hashStringToUint64 computes an FNV-1a 64-bit hash of a string.
func hashStringToUint64(s string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	const prime uint64 = 0x100000001b3
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}
	return hash
}
