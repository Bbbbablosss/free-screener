package main

// wsDecode transparently decompresses a WS frame that an exchange's "compress"
// endpoint may send as application-level gzip / zlib / raw-DEFLATE (e.g. BitMart
// spot ws-manager-compress, HTX, BingX). If `raw` is already plain text/JSON it is
// returned unchanged. Safe to call on every frame. Uses POOLED decoders (decompress.go)
// so it does NOT allocate a fresh ~32KB codec window per frame.
func wsDecode(raw []byte) []byte {
	if len(raw) < 2 {
		return raw
	}
	switch {
	case raw[0] == 0x1f && raw[1] == 0x8b: // gzip magic
		if out, err := gunzipPooled(raw); err == nil && len(out) > 0 {
			return out
		}
	case raw[0] == 0x78: // zlib (78 01 / 9c / da)
		if out, err := unzlibPooled(raw); err == nil && len(out) > 0 {
			return out
		}
	case raw[0] != '{' && raw[0] != '[': // not JSON → try raw DEFLATE (no header)
		if out, err := inflatePooled(raw); err == nil && len(out) > 0 && (out[0] == '{' || out[0] == '[') {
			return out
		}
	}
	return raw
}
