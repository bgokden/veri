package gencoder

import (
	"io"
	"time"
	"unsafe"

	"github.com/bgokden/veri/models"
	pb "github.com/bgokden/veri/veriservice"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

//////////
func SizeKey(d *pb.DatumKey) (s uint64) {

	{
		l := uint64(len(d.Feature))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		s += 4 * l

	}
	{
		l := uint64(len(d.GroupLabel))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 16
	return
}

func MarshalKey(d *pb.DatumKey) ([]byte, error) {
	size := SizeKey(d)
	buf := make([]byte, size)
	i := uint64(0)

	{
		l := uint64(len(d.Feature))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k0 := range d.Feature {

			{

				v := *(*uint32)(unsafe.Pointer(&(d.Feature[k0])))

				buf[i+0+0] = byte(v >> 0)

				buf[i+1+0] = byte(v >> 8)

				buf[i+2+0] = byte(v >> 16)

				buf[i+3+0] = byte(v >> 24)

			}

			i += 4

		}
	}
	{
		l := uint64(len(d.GroupLabel))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.GroupLabel)
		i += l
	}
	{

		buf[i+0+0] = byte(d.Size1 >> 0)

		buf[i+1+0] = byte(d.Size1 >> 8)

		buf[i+2+0] = byte(d.Size1 >> 16)

		buf[i+3+0] = byte(d.Size1 >> 24)

	}
	{

		buf[i+0+4] = byte(d.Size2 >> 0)

		buf[i+1+4] = byte(d.Size2 >> 8)

		buf[i+2+4] = byte(d.Size2 >> 16)

		buf[i+3+4] = byte(d.Size2 >> 24)

	}
	{

		buf[i+0+8] = byte(d.Dim1 >> 0)

		buf[i+1+8] = byte(d.Dim1 >> 8)

		buf[i+2+8] = byte(d.Dim1 >> 16)

		buf[i+3+8] = byte(d.Dim1 >> 24)

	}
	{

		buf[i+0+12] = byte(d.Dim2 >> 0)

		buf[i+1+12] = byte(d.Dim2 >> 8)

		buf[i+2+12] = byte(d.Dim2 >> 16)

		buf[i+3+12] = byte(d.Dim2 >> 24)

	}
	return buf[:i+16], nil
}

func UnmarshalKey(d *pb.DatumKey, buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Feature)) >= l {
			d.Feature = d.Feature[:l]
		} else {
			d.Feature = make([]float32, l)
		}
		for k0 := range d.Feature {

			{

				v := 0 | (uint32(buf[i+0+0]) << 0) | (uint32(buf[i+1+0]) << 8) | (uint32(buf[i+2+0]) << 16) | (uint32(buf[i+3+0]) << 24)
				d.Feature[k0] = *(*float32)(unsafe.Pointer(&v))

			}

			i += 4

		}
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.GroupLabel)) >= l {
			d.GroupLabel = d.GroupLabel[:l]
		} else {
			d.GroupLabel = make([]byte, l)
		}
		copy(d.GroupLabel, buf[i+0:])
		i += l
	}
	{

		d.Size1 = 0 | (uint32(buf[i+0+0]) << 0) | (uint32(buf[i+1+0]) << 8) | (uint32(buf[i+2+0]) << 16) | (uint32(buf[i+3+0]) << 24)

	}
	{

		d.Size2 = 0 | (uint32(buf[i+0+4]) << 0) | (uint32(buf[i+1+4]) << 8) | (uint32(buf[i+2+4]) << 16) | (uint32(buf[i+3+4]) << 24)

	}
	{

		d.Dim1 = 0 | (uint32(buf[i+0+8]) << 0) | (uint32(buf[i+1+8]) << 8) | (uint32(buf[i+2+8]) << 16) | (uint32(buf[i+3+8]) << 24)

	}
	{

		d.Dim2 = 0 | (uint32(buf[i+0+12]) << 0) | (uint32(buf[i+1+12]) << 8) | (uint32(buf[i+2+12]) << 16) | (uint32(buf[i+3+12]) << 24)

	}
	return i + 16, nil
}

////////////////////////

func SizeValue(d *pb.DatumValue) (s uint64) {

	{
		l := uint64(len(d.Label))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 8
	return
}
func MarshalValue(d *pb.DatumValue) ([]byte, error) {
	size := SizeValue(d)
	buf := make([]byte, size)
	i := uint64(0)

	{

		buf[0+0] = byte(d.Version >> 0)

		buf[1+0] = byte(d.Version >> 8)

		buf[2+0] = byte(d.Version >> 16)

		buf[3+0] = byte(d.Version >> 24)

		buf[4+0] = byte(d.Version >> 32)

		buf[5+0] = byte(d.Version >> 40)

		buf[6+0] = byte(d.Version >> 48)

		buf[7+0] = byte(d.Version >> 56)

	}
	{
		l := uint64(len(d.Label))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+8] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+8] = byte(t)
			i++

		}
		copy(buf[i+8:], d.Label)
		i += l
	}
	return buf[:i+8], nil
}

func UnmarshalValue(d *pb.DatumValue, buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.Version = 0 | (uint64(buf[i+0+0]) << 0) | (uint64(buf[i+1+0]) << 8) | (uint64(buf[i+2+0]) << 16) | (uint64(buf[i+3+0]) << 24) | (uint64(buf[i+4+0]) << 32) | (uint64(buf[i+5+0]) << 40) | (uint64(buf[i+6+0]) << 48) | (uint64(buf[i+7+0]) << 56)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+8] & 0x7F)
			for buf[i+8]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+8]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Label)) >= l {
			d.Label = d.Label[:l]
		} else {
			d.Label = make([]byte, l)
		}
		copy(d.Label, buf[i+8:])
		i += l
	}
	return i + 8, nil
}

type DatumScore struct {
	Score float64
}

func (d *DatumScore) Size() (s uint64) {

	s += 8
	return
}
func (d *DatumScore) Marshal() ([]byte, error) {
	size := d.Size()
	buf := make([]byte, size)
	i := uint64(0)

	{

		v := *(*uint64)(unsafe.Pointer(&(d.Score)))

		buf[0+0] = byte(v >> 0)

		buf[1+0] = byte(v >> 8)

		buf[2+0] = byte(v >> 16)

		buf[3+0] = byte(v >> 24)

		buf[4+0] = byte(v >> 32)

		buf[5+0] = byte(v >> 40)

		buf[6+0] = byte(v >> 48)

		buf[7+0] = byte(v >> 56)

	}
	return buf[:i+8], nil
}

func (d *DatumScore) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		v := 0 | (uint64(buf[0+0]) << 0) | (uint64(buf[1+0]) << 8) | (uint64(buf[2+0]) << 16) | (uint64(buf[3+0]) << 24) | (uint64(buf[4+0]) << 32) | (uint64(buf[5+0]) << 40) | (uint64(buf[6+0]) << 48) | (uint64(buf[7+0]) << 56)
		d.Score = *(*float64)(unsafe.Pointer(&v))

	}
	return i + 8, nil
}

////// InternalDatumKey
func SizeInternalKey(d *models.InternalDatumKey) (s uint64) {

	{
		l := uint64(len(d.Feature))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		s += 4 * l

	}
	{
		l := uint64(len(d.GroupLabel))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	s += 16
	return
}

func MarshalInternalKey(d *models.InternalDatumKey) ([]byte, error) {
	size := SizeInternalKey(d)
	buf := make([]byte, size)
	i := uint64(0)

	{
		l := uint64(len(d.Feature))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		for k0 := range d.Feature {

			{

				v := *(*uint32)(unsafe.Pointer(&(d.Feature[k0])))

				buf[i+0+0] = byte(v >> 0)

				buf[i+1+0] = byte(v >> 8)

				buf[i+2+0] = byte(v >> 16)

				buf[i+3+0] = byte(v >> 24)

			}

			i += 4

		}
	}
	{
		l := uint64(len(d.GroupLabel))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+0] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+0] = byte(t)
			i++

		}
		copy(buf[i+0:], d.GroupLabel)
		i += l
	}
	{

		buf[i+0+0] = byte(d.Size1 >> 0)

		buf[i+1+0] = byte(d.Size1 >> 8)

		buf[i+2+0] = byte(d.Size1 >> 16)

		buf[i+3+0] = byte(d.Size1 >> 24)

	}
	{

		buf[i+0+4] = byte(d.Size2 >> 0)

		buf[i+1+4] = byte(d.Size2 >> 8)

		buf[i+2+4] = byte(d.Size2 >> 16)

		buf[i+3+4] = byte(d.Size2 >> 24)

	}
	{

		buf[i+0+8] = byte(d.Dim1 >> 0)

		buf[i+1+8] = byte(d.Dim1 >> 8)

		buf[i+2+8] = byte(d.Dim1 >> 16)

		buf[i+3+8] = byte(d.Dim1 >> 24)

	}
	{

		buf[i+0+12] = byte(d.Dim2 >> 0)

		buf[i+1+12] = byte(d.Dim2 >> 8)

		buf[i+2+12] = byte(d.Dim2 >> 16)

		buf[i+3+12] = byte(d.Dim2 >> 24)

	}
	return buf[:i+16], nil
}

func UnmarshalInternalKey(d *models.InternalDatumKey, buf []byte) (uint64, error) {
	i := uint64(0)

	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Feature)) >= l {
			d.Feature = d.Feature[:l]
		} else {
			d.Feature = make([]float32, l)
		}
		for k0 := range d.Feature {

			{

				v := 0 | (uint32(buf[i+0+0]) << 0) | (uint32(buf[i+1+0]) << 8) | (uint32(buf[i+2+0]) << 16) | (uint32(buf[i+3+0]) << 24)
				d.Feature[k0] = *(*float32)(unsafe.Pointer(&v))

			}

			i += 4

		}
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+0] & 0x7F)
			for buf[i+0]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+0]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.GroupLabel)) >= l {
			d.GroupLabel = d.GroupLabel[:l]
		} else {
			d.GroupLabel = make([]byte, l)
		}
		copy(d.GroupLabel, buf[i+0:])
		i += l
	}
	{

		d.Size1 = 0 | (uint32(buf[i+0+0]) << 0) | (uint32(buf[i+1+0]) << 8) | (uint32(buf[i+2+0]) << 16) | (uint32(buf[i+3+0]) << 24)

	}
	{

		d.Size2 = 0 | (uint32(buf[i+0+4]) << 0) | (uint32(buf[i+1+4]) << 8) | (uint32(buf[i+2+4]) << 16) | (uint32(buf[i+3+4]) << 24)

	}
	{

		d.Dim1 = 0 | (uint32(buf[i+0+8]) << 0) | (uint32(buf[i+1+8]) << 8) | (uint32(buf[i+2+8]) << 16) | (uint32(buf[i+3+8]) << 24)

	}
	{

		d.Dim2 = 0 | (uint32(buf[i+0+12]) << 0) | (uint32(buf[i+1+12]) << 8) | (uint32(buf[i+2+12]) << 16) | (uint32(buf[i+3+12]) << 24)

	}
	return i + 16, nil
}

////////////////////////
