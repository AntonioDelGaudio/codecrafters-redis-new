package main

import (
	"bufio"
	"encoding/binary"
	"math"
	"time"
)

const version = "REDIS0011"
const hashTableFlag = 0xfb

const (
	rdbVersion  = 9
	rdb6bitLen  = 0
	rdb14bitLen = 1
	rdb32bitLen = 0x80
	rdb64bitLen = 0x81
	rdbEncVal   = 3
	rdbLenErr   = math.MaxUint64

	rdbOpCodeModuleAux = 247
	rdbOpCodeIdle      = 248
	rdbOpCodeFreq      = 249
	rdbOpCodeAux       = 250
	rdbOpCodeResizeDB  = 251
	rdbOpCodeExpiryMS  = 252
	rdbOpCodeExpiry    = 253
	rdbOpCodeSelectDB  = 254
	rdbOpCodeEOF       = 255

	rdbModuleOpCodeEOF    = 0
	rdbModuleOpCodeSint   = 1
	rdbModuleOpCodeUint   = 2
	rdbModuleOpCodeFloat  = 3
	rdbModuleOpCodeDouble = 4
	rdbModuleOpCodeString = 5

	rdbLoadNone  = 0
	rdbLoadEnc   = 1 << 0
	rdbLoadPlain = 1 << 1
	rdbLoadSds   = 1 << 2

	rdbSaveNode        = 0
	rdbSaveAofPreamble = 1 << 0

	rdbEncInt8  = 0
	rdbEncInt16 = 1
	rdbEncInt32 = 2
	rdbEncLZF   = 3

	rdbZiplist6bitlenString  = 0
	rdbZiplist14bitlenString = 1
	rdbZiplist32bitlenString = 2

	rdbZiplistInt16 = 0xc0
	rdbZiplistInt32 = 0xd0
	rdbZiplistInt64 = 0xe0
	rdbZiplistInt24 = 0xf0
	rdbZiplistInt8  = 0xfe
	rdbZiplistInt4  = 15

	rdbLpHdrSize           = 6
	rdbLpHdrNumeleUnknown  = math.MaxUint16
	rdbLpMaxIntEncodingLen = 0
	rdbLpMaxBacklenSize    = 5
	rdbLpMaxEntryBacklen   = 34359738367
	rdbLpEncodingInt       = 0
	rdbLpEncodingString    = 1

	rdbLpEncoding7BitUint     = 0
	rdbLpEncoding7BitUintMask = 0x80

	rdbLpEncoding6BitStr     = 0x80
	rdbLpEncoding6BitStrMask = 0xC0

	rdbLpEncoding13BitInt     = 0xC0
	rdbLpEncoding13BitIntMask = 0xE0

	rdbLpEncoding12BitStr     = 0xE0
	rdbLpEncoding12BitStrMask = 0xF0

	rdbLpEncoding16BitInt     = 0xF1
	rdbLpEncoding16BitIntMask = 0xFF

	rdbLpEncoding24BitInt     = 0xF2
	rdbLpEncoding24BitIntMask = 0xFF

	rdbLpEncoding32BitInt     = 0xF3
	rdbLpEncoding32BitIntMask = 0xFF

	rdbLpEncoding64BitInt     = 0xF4
	rdbLpEncoding64BitIntMask = 0xFF

	rdbLpEncoding32BitStr     = 0xF0
	rdbLpEncoding32BitStrMask = 0xFF

	rdbLpEOF = 0xFF
)

func parseDb(r *bufio.Reader) map[string]Record {
	m := make(map[string]Record)
	// check version
	v := make([]byte, 9)
	_, err := r.Read(v)
	if err != nil {
		panic(err)
	}
	if string(v) != version {
		panic("Incorrect version, i'm going boom")
	}
	// find memory
	_, _ = r.ReadBytes(hashTableFlag) // ignore metadata, reach db data
	l, err := r.ReadByte()            //number of keys without expiry
	if err != nil {
		panic(err)
	}
	_, err = r.ReadByte() //number of keys with expiry, i don't need it for now
	if err != nil {
		panic(err)
	}
	for range l {
		valueTypeByte, err := r.ReadByte()
		var ttl time.Duration
		var expiry bool
		if err != nil {
			panic(err)
		}
		now := time.Now()
		switch valueTypeByte {
		case 0x00:
			expiry = false
		case 0xFC:
			expiry = true
			ttl = decodeMillisecondsTimetamp(r, now)
			valueTypeByte, err = r.ReadByte()
		case 0xFD:
			expiry = true
			ttl = decodeSecondsTimestamp(r, now)
			valueTypeByte, err = r.ReadByte()
		}
		k := decodeStringEncoding(r)
		v := decodeStringEncoding(r)
		m[k] = Record{
			value:    v,
			expiry:   expiry,
			ttl:      ttl,
			modified: now,
		}
	}
	return m
}

func decodeMillisecondsTimetamp(r *bufio.Reader, now time.Time) time.Duration {
	b := make([]byte, 8)
	_, err := r.Read(b)
	if err != nil {
		panic(err)
	}
	timestamp := int64(binary.LittleEndian.Uint64(b))
	timestamp = timestamp / 1000
	ttl := time.Unix(timestamp, 0).Sub(now)
	return ttl
}

func decodeSecondsTimestamp(r *bufio.Reader, now time.Time) time.Duration {
	b := make([]byte, 4)
	_, err := r.Read(b)
	if err != nil {
		panic(err)
	}
	ttl := time.Unix(int64(binary.LittleEndian.Uint32(b)), 0).Sub(now)
	return ttl
}

func decodeStringEncoding(r *bufio.Reader) string {
	var s string
	var l int
	fb, err := r.ReadByte()
	if err != nil {
		panic(err)
	}
	if fb>>7 == 0 && fb>>6 == 0 {
		l = int(fb)
	}
	if fb>>7 == 0 && fb>>6 == 1 {
		fb = fb - 64
		nl, err := r.ReadByte()
		if err != nil {
			panic(err)
		}
		totL := make([]byte, 2)
		totL[0] = fb
		totL[1] = nl
		l = int(binary.BigEndian.Uint16(totL))
	}
	if l>>7 == 1 && l>>6 == 0 {
		rl := make([]byte, 4)
		_, err = r.Read(rl)
		l = int(binary.BigEndian.Uint32(rl))
	}
	if l>>7 == 1 && l>>6 == 1 {
		panic("Allah Akbar")
	}
	v := make([]byte, l)
	_, err = r.Read(v)
	if err != nil {
		panic(err)
	}
	s = string(v)
	return s
}

func decodeKeyValueExpiry(r *bufio.Reader) map[string]Record {
	m := make(map[string]Record)

	return m
}
