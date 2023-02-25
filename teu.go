package wal

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	SOT64B = SOT(1 << (6 + iota))
	SOT128B
	SOT256B
	SOT512B
	SOT1K
	SOT2K
	SOT4K
	SOT8K
)

type SOT uint16

func (sot SOT) value() uint16 {
	return uint16(sot)
}

func (sot SOT) String() string {
	switch sot {
	case SOT64B:
		return "SOT64B"
	case SOT128B:
		return "SOT128B"
	case SOT256B:
		return "SOT256B"
	case SOT512B:
		return "SOT512B"
	case SOT1K:
		return "SOT1K"
	case SOT2K:
		return "SOT2K"
	case SOT4K:
		return "SOT4K"
	case SOT8K:
		return "SOT8K"
	default:
		return fmt.Sprintf("Unknonw(%d)", sot)
	}
}

func NewTEU(size SOT) (teu TEU) {
	teu = make([]byte, size)
	teu.setSize(size)
	return teu
}

type TEU []byte

func (teu TEU) setSize(size SOT) {
	binary.BigEndian.PutUint16(teu[0:2], size.value())
	return
}

func (teu TEU) sot() (size SOT) {
	size = SOT(binary.BigEndian.Uint16(teu[0:2]))
	return
}

func (teu TEU) setIndex(idx uint16) {
	binary.BigEndian.PutUint16(teu[2:4], idx)
	return
}

func (teu TEU) index() (idx uint16) {
	idx = binary.BigEndian.Uint16(teu[2:4])
	return
}

func (teu TEU) setSpan(span uint16) {
	binary.BigEndian.PutUint16(teu[4:6], span)
	return
}

func (teu TEU) span() (span uint16) {
	span = binary.BigEndian.Uint16(teu[4:6])
	return
}

func (teu TEU) setData(p []byte) {
	binary.BigEndian.PutUint16(teu[6:8], uint16(len(p)))
	copy(teu[8:], p)
	return
}

func (teu TEU) data() (p []byte) {
	p = teu[8 : 8+binary.BigEndian.Uint16(teu[6:8])]
	return
}

func (teu TEU) validate() (ok bool) {
	ok = teu.span() > 0
	return
}

func NewTEUs(size SOT, p []byte) (b TEUs) {
	pLen := len(p)
	span := uint16(math.Ceil(float64(pLen) / float64(size.value()-8)))
	b = make([]byte, span*size.value())
	low := uint16(0)
	high := size.value() - 8
	for i := uint16(0); i < span; i++ {
		teu := TEU(b[i*size.value() : (i+1)*size.value()])
		teu.setSize(size)
		teu.setIndex(i)
		teu.setSpan(span)
		if i == span-1 {
			teu.setData(p[low:])
		} else {
			teu.setData(p[low:high])
		}
		low = high
		high = low + high
	}
	return
}

type TEUs []byte

func (s TEUs) Len() (p uint16) {
	p = TEU(s).span()
	return
}

func (s TEUs) SOT() (sot SOT) {
	sot = TEU(s).sot()
	return
}

func (s TEUs) FirstDataCut(low int, high int) (p []byte) {
	p = s[8+low : 8+high]
	return
}

func (s TEUs) Data() (p []byte) {
	p = make([]byte, 0, len(s))
	sot := s.SOT().value()
	span := s.Len()
	for i := uint16(0); i < span; i++ {
		teu := TEU(s[i*sot : (i+1)*sot])
		p = append(p, teu.data()...)
	}
	return
}
