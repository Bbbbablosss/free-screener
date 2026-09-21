package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"sync"
)

// Pooled WS-frame decompressors. Exchanges' "compress" endpoints send gzip / zlib /
// raw-DEFLATE frames; creating a NEW reader per frame allocates the codec's ~32 KB
// sliding window EVERY message — profiled (perf) as the dominant mallocgcLarge/memclr
// on the gzip ingest path (bingx/htx/bitmart). These pools reuse a reader via Reset →
// near-zero per-frame allocation. sync.Pool is goroutine-safe across connection loops.

var gzipPool = sync.Pool{New: func() any { return new(gzip.Reader) }}

func gunzipPooled(b []byte) ([]byte, error) {
	zr := gzipPool.Get().(*gzip.Reader)
	defer gzipPool.Put(zr)
	if err := zr.Reset(bytes.NewReader(b)); err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

var flatePool = sync.Pool{New: func() any { return flate.NewReader(bytes.NewReader(nil)) }}

func inflatePooled(b []byte) ([]byte, error) {
	fr := flatePool.Get().(io.ReadCloser)
	defer flatePool.Put(fr)
	if err := fr.(flate.Resetter).Reset(bytes.NewReader(b), nil); err != nil {
		return nil, err
	}
	return io.ReadAll(fr)
}

var zlibPool = sync.Pool{New: func() any { return nil }}

func unzlibPooled(b []byte) ([]byte, error) {
	if v := zlibPool.Get(); v != nil {
		zr := v.(io.ReadCloser)
		defer zlibPool.Put(zr)
		if err := zr.(zlib.Resetter).Reset(bytes.NewReader(b), nil); err != nil {
			return nil, err
		}
		return io.ReadAll(zr)
	}
	zr, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer zlibPool.Put(zr)
	return io.ReadAll(zr)
}
