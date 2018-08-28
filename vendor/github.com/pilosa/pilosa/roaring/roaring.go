// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package roaring implements roaring bitmaps with support for incremental changes.
package roaring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/bits"
	"sort"
	"unsafe"
)

const (
	// magicNumber is an identifier, in bytes 0-1 of the file.
	magicNumber = uint32(12348)

	// storageVersion indicates the storage version, in bytes 2-3.
	storageVersion = uint32(0)

	// cookie is the first four bytes in a roaring bitmap file,
	// formed by joining magicNumber and storageVersion
	cookie = magicNumber + storageVersion<<16

	// headerBaseSize is the size in bytes of the cookie and key count at the
	// beginning of a file.
	headerBaseSize = 4 + 4

	// runCountHeaderSize is the size in bytes of the run count stored
	// at the beginning of every serialized run container.
	runCountHeaderSize = 2

	// interval32Size is the size of a single run in a container.runs.
	interval16Size = 4

	// bitmapN is the number of values in a container.bitmap.
	bitmapN = (1 << 16) / 64

	//containerArray indicates a container of bit position values
	containerArray = byte(1)

	//containerBitmap indicates a container of bits packed in a uint64 array block
	containerBitmap = byte(2)

	//containerRun  indicates a container of run encoded bits
	containerRun = byte(3)

	maxContainerVal = 0xffff
)

type Containers interface {
	// Get returns nil if the key does not exist.
	Get(key uint64) *Container

	// Put adds the container at key.
	Put(key uint64, c *Container)

	// PutContainerValues updates an existing container at key.
	// If a container does not exist for key, a new one is allocated.
	PutContainerValues(key uint64, containerType byte, n int, mapped bool)

	// Remove takes the container at key out.
	Remove(key uint64)

	// GetOrCreate returns the container at key, creating a new empty container if necessary.
	GetOrCreate(key uint64) *Container

	// Clone does a deep copy of Containers, including cloning all containers contained.
	Clone() Containers

	// Last returns the highest key and associated container.
	Last() (key uint64, c *Container)

	// Size returns the number of containers stored.
	Size() int

	// Iterator returns a Contiterator which after a call to Next(), a call to Value() will
	// return the first container at or after key. found will be true if a
	// container is found at key.
	Iterator(key uint64) (citer ContainerIterator, found bool)

	Count() uint64

	//Reset will clear the containers collection to allow for recycling during snapshot
	Reset()
}

type ContainerIterator interface {
	Next() bool
	Value() (uint64, *Container)
}

// Bitmap represents a roaring bitmap.
type Bitmap struct {
	Containers Containers

	// Number of operations written to the writer.
	opN int

	// Writer where operations are appended to.
	OpWriter io.Writer
}

// NewBitmap returns a Bitmap with an initial set of values.
func NewBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newSliceContainers(),
	}
	b.Add(a...)
	return b
}

// NewFileBitmap returns a Bitmap with an initial set of values, used for file storage.
// By default, this is a copy of NewBitmap, but is replaced with B+Tree in server/enterprise.go
var NewFileBitmap func(a ...uint64) *Bitmap = NewBitmap

// Clone returns a heap allocated copy of the bitmap.
// Note: The OpWriter IS NOT copied to the new bitmap.
func (b *Bitmap) Clone() *Bitmap {
	if b == nil {
		return nil
	}

	// Create a copy of the bitmap structure.
	other := &Bitmap{
		Containers: b.Containers.Clone(),
	}

	return other
}

// Add adds values to the bitmap.
func (b *Bitmap) Add(a ...uint64) (changed bool, err error) {
	changed = false
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeAdd, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return false, err
		}

		// Apply to the in-memory bitmap.
		if op.apply(b) {
			changed = true

		}
	}

	return changed, nil
}

func (b *Bitmap) add(v uint64) bool {
	cont := b.Containers.GetOrCreate(highbits(v))
	return cont.add(lowbits(v))
}

// Contains returns true if v is in the bitmap.
func (b *Bitmap) Contains(v uint64) bool {
	c := b.Containers.Get(highbits(v))
	if c == nil {
		return false
	}
	return c.contains(lowbits(v))
}

// Remove removes values from the bitmap.
func (b *Bitmap) Remove(a ...uint64) (changed bool, err error) {
	changed = false
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeRemove, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return false, err
		}

		// Apply operation to the bitmap.
		if op.apply(b) {
			changed = true
		}
	}
	return changed, nil
}

func (b *Bitmap) remove(v uint64) bool {
	c := b.Containers.Get(highbits(v))
	if c == nil {
		return false
	}
	// TODO - do nil check inside c.remove?
	return c.remove(lowbits(v))
}

// Max returns the highest value in the bitmap.
// Returns zero if the bitmap is empty.
func (b *Bitmap) Max() uint64 {
	if b.Containers.Size() == 0 {
		return 0
	}

	hb, c := b.Containers.Last()
	lb := c.max()
	return hb<<16 | uint64(lb)
}

// Count returns the number of bits set in the bitmap.
func (b *Bitmap) Count() (n uint64) {
	return b.Containers.Count()
}

// CountRange returns the number of bits set between [start, end).
func (b *Bitmap) CountRange(start, end uint64) (n uint64) {
	if b.Containers.Size() == 0 {
		return
	}

	skey := highbits(start)
	ekey := highbits(end)

	citer, found := b.Containers.Iterator(highbits(start))
	// If range is entirely in one container then just count that range.
	if found && skey == ekey {
		citer.Next()
		_, c := citer.Value()
		return uint64(c.countRange(int(lowbits(start)), int(lowbits(end))))
	}

	for citer.Next() {
		k, c := citer.Value()
		if k < skey {
			// TODO remove once we've validated this stuff works
			panic("should be impossible for k to be less than skey")
		}
		if k == skey {
			n += uint64(c.countRange(int(lowbits(start)), maxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.n)
			continue
		}
		if k == ekey {
			n += uint64(c.countRange(0, int(lowbits(end))))
			break
		}
		if k > ekey {
			break
		}
	}
	return n
}

// Slice returns a slice of all integers in the bitmap.
func (b *Bitmap) Slice() []uint64 {
	var a []uint64
	itr := b.Iterator()
	itr.Seek(0)

	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// SliceRange returns a slice of integers between [start, end).
func (b *Bitmap) SliceRange(start, end uint64) []uint64 {
	var a []uint64
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// ForEach executes fn for each value in the bitmap.
func (b *Bitmap) ForEach(fn func(uint64)) {
	itr := b.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		fn(v)
	}
}

// ForEachRange executes fn for each value in the bitmap between [start, end).
func (b *Bitmap) ForEachRange(start, end uint64, fn func(uint64)) {
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		fn(v)
	}
}

// OffsetRange returns a new bitmap with a containers offset by start.
func (b *Bitmap) OffsetRange(offset, start, end uint64) *Bitmap {
	if lowbits(offset) != 0 {
		panic("offset must not contain low bits")
	}
	if lowbits(start) != 0 {
		panic("range start must not contain low bits")
	}
	if lowbits(end) != 0 {
		panic("range end must not contain low bits")
	}

	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(end)
	citer, _ := b.Containers.Iterator(hi0)
	other := NewBitmap()
	for citer.Next() {
		k, c := citer.Value()
		if k >= hi1 {
			break
		}
		other.Containers.Put(off+(k-hi0), c)
	}
	return other
}

// container returns the container with the given key.
func (b *Bitmap) container(key uint64) *Container {
	return b.Containers.Get(key)
}

// IntersectionCount returns the number of set bits that would result in an
// intersection between b and other. It is more efficient than actually
// intersecting the two and counting the result.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	var n uint64
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i && j {
		if ki < kj {
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if ki > kj {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else {
			n += uint64(intersectionCount(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return n
}

// Intersect returns the intersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	output := NewBitmap()
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i && j {
		if ki < kj {
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if ki > kj {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, intersect(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Union returns the bitwise union of b and other.
func (b *Bitmap) Union(other *Bitmap) *Bitmap {
	output := NewBitmap()

	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Clone())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			output.Containers.Put(kj, cj.Clone())
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, union(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Difference returns the difference of b and other.
func (b *Bitmap) Difference(other *Bitmap) *Bitmap {
	output := NewBitmap()

	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Clone())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, difference(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Xor returns the bitwise exclusive or of b and other.
func (b *Bitmap) Xor(other *Bitmap) *Bitmap {
	output := NewBitmap()

	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Clone())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			output.Containers.Put(kj, cj.Clone())
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, xor(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// removeEmptyContainers deletes all containers that have a count of zero.
func (b *Bitmap) removeEmptyContainers() {
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		if c.n == 0 {
			b.Containers.Remove(k)
		}
	}
}
func (b *Bitmap) countEmptyContainers() int {
	result := 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.n == 0 {
			result++
		}
	}
	return result
}

// Optimize converts array and bitmap containers to run containers as necessary.
func (b *Bitmap) Optimize() {
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		c.optimize()
	}
}

type errWriter struct {
	w   io.Writer
	err error
	n   int
}

func (ew *errWriter) WriteUint16(b []byte, v uint16) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint16(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}
func (ew *errWriter) WriteUint32(b []byte, v uint32) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint32(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}

func (ew *errWriter) WriteUint64(b []byte, v uint64) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint64(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}

// WriteTo writes b to w.
func (b *Bitmap) WriteTo(w io.Writer) (n int64, err error) {
	b.Optimize()
	// Remove empty containers before persisting.
	//b.removeEmptyContainers()

	containerCount := b.Containers.Size() - b.countEmptyContainers()
	headerSize := headerBaseSize
	byte2 := make([]byte, 2)
	byte4 := make([]byte, 4)
	byte8 := make([]byte, 8)

	// Build header before writing individual container blocks.
	// Metadata for each container is 8+2+2+4 = sizeof(key) + sizeof(containerType)+sizeof(cardinality) + sizeof(file offset)
	// Cookie header section.
	ew := &errWriter{
		w: w,
		n: 0,
	}

	ew.WriteUint32(byte4, cookie)
	ew.WriteUint32(byte4, uint32(containerCount))

	// Descriptive header section: encode keys and cardinality.
	// Key and cardinality are stored interleaved here, 12 bytes per container.
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		key, c := citer.Value()
		// Verify container count before writing.
		// TODO: instead of commenting this out, we need to make it a configuration option
		//count := c.count()
		//assert(c.count() == c.n, "cannot write container count, mismatch: count=%d, n=%d", count, c.n)
		if c.n > 0 {
			ew.WriteUint64(byte8, key)
			ew.WriteUint16(byte2, uint16(c.containerType))
			ew.WriteUint16(byte2, uint16(c.n-1))
		}

	}

	// Offset header section: write the offset for each container block.
	// 4 bytes per container.
	offset := uint32(headerSize + (containerCount * (8 + 2 + 2 + 4)))
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.n > 0 {
			ew.WriteUint32(byte4, offset)
			offset += uint32(c.size())
		}

	}
	if ew.err != nil {
		return int64(ew.n), ew.err
	}

	n = int64(headerSize + (containerCount * (8 + 2 + 2 + 4)))

	// Container storage section: write each container block.
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.n > 0 {
			nn, err := c.WriteTo(w)
			n += nn
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// UnmarshalBinary decodes b from a binary-encoded byte slice.
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	if len(data) < headerBaseSize {
		return errors.New("data too small")
	}

	// Verify the first two bytes are a valid magicNumber, and second two bytes match current storageVersion.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	fileVersion := uint32(binary.LittleEndian.Uint16(data[2:4]))
	if fileMagic != magicNumber {
		return fmt.Errorf("invalid roaring file, magic number %v is incorrect", fileMagic)
	}

	if fileVersion != storageVersion {
		return fmt.Errorf("wrong roaring version, file is v%d, server requires v%d", fileVersion, storageVersion)
	}

	// Read key count in bytes sizeof(cookie):(sizeof(cookie)+sizeof(uint32)).
	keyN := binary.LittleEndian.Uint32(data[4:8])

	headerSize := headerBaseSize
	b.Containers.Reset()
	// Descriptive header section: Read container keys and cardinalities.
	for i, buf := 0, data[headerSize:]; i < int(keyN); i, buf = i+1, buf[12:] {
		b.Containers.PutContainerValues(
			binary.LittleEndian.Uint64(buf[0:8]),
			byte(binary.LittleEndian.Uint16(buf[8:10])),
			int(binary.LittleEndian.Uint16(buf[10:12]))+1,
			true)
	}
	opsOffset := headerSize + int(keyN)*12

	// Read container offsets and attach data.
	citer, _ := b.Containers.Iterator(0)
	for i, buf := 0, data[opsOffset:]; i < int(keyN); i, buf = i+1, buf[4:] {
		offset := binary.LittleEndian.Uint32(buf[0:4])
		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		citer.Next()
		_, c := citer.Value()
		switch c.containerType {
		case containerRun:
			c.array = nil
			c.bitmap = nil
			runCount := binary.LittleEndian.Uint16(data[offset : offset+runCountHeaderSize])
			c.runs = (*[0xFFFFFFF]interval16)(unsafe.Pointer(&data[offset+runCountHeaderSize]))[:runCount]
			opsOffset = int(offset) + runCountHeaderSize + len(c.runs)*interval16Size
		case containerArray:
			c.runs = nil
			c.bitmap = nil
			c.array = (*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[offset]))[:c.n]
			opsOffset = int(offset) + len(c.array)*2 // sizeof(uint32)
		case containerBitmap:
			c.array = nil
			c.runs = nil
			c.bitmap = (*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN]
			opsOffset = int(offset) + len(c.bitmap)*8 // sizeof(uint64)
		}
	}

	// Read ops log until the end of the file.
	buf := data[opsOffset:]
	for {
		// Exit when there are no more ops to parse.
		if len(buf) == 0 {
			break
		}

		// Unmarshal the op and apply it.
		var opr op
		if err := opr.UnmarshalBinary(buf); err != nil {
			// FIXME(benbjohnson): return error with position so file can be trimmed.
			return err
		}

		opr.apply(b)

		// Increase the op count.
		b.opN++

		// Move the buffer forward.
		buf = buf[opr.size():]
	}

	return nil
}

// writeOp writes op to the OpWriter, if available.
func (b *Bitmap) writeOp(op *op) error {
	if b.OpWriter == nil {
		return nil
	}

	if _, err := op.WriteTo(b.OpWriter); err != nil {
		return err
	}

	b.opN++
	return nil
}

// Iterator returns a new iterator for the bitmap.
func (b *Bitmap) Iterator() *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(0)
	return itr
}

// Info returns stats for the bitmap.
func (b *Bitmap) Info() bitmapInfo {
	info := bitmapInfo{
		OpN:        b.opN,
		Containers: make([]containerInfo, 0, b.Containers.Size()),
	}

	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		ci := c.info()
		ci.Key = k
		info.Containers = append(info.Containers, ci)
	}
	return info
}

// Check performs a consistency check on the bitmap. Returns nil if consistent.
func (b *Bitmap) Check() error {
	var a ErrorList

	// Check each container.
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		if err := c.check(); err != nil {
			a.AppendWithPrefix(err, fmt.Sprintf("%d/", k))
		}
	}

	if len(a) == 0 {
		return nil
	}
	return a
}

// Flip performs a logical negate of the bits in the range [start,end].
func (b *Bitmap) Flip(start, end uint64) *Bitmap {
	result := NewBitmap()
	itr := b.Iterator()
	v, eof := itr.Next()
	//copy over previous bits.
	for v < start && !eof {
		result.add(v)
		v, eof = itr.Next()
	}
	//flip bits in range .
	for i := start; i <= end; i++ {
		if eof {
			result.add(i)
		} else if v == i {
			v, eof = itr.Next()
		} else {
			result.add(i)
		}
	}
	//add remaining.
	for !eof {
		result.add(v)
		v, eof = itr.Next()
	}
	return result
}

// bitmapInfo represents a point-in-time snapshot of bitmap stats.
type bitmapInfo struct {
	OpN        int
	Containers []containerInfo
}

// Iterator represents an iterator over a Bitmap.
type Iterator struct {
	bitmap *Bitmap
	citer  ContainerIterator
	key    uint64
	c      *Container
	j, k   int // i: container; j: array index, bit index, or run index; k: offset within the run
}

// Seek moves to the first value equal to or greater than `seek`.
func (itr *Iterator) Seek(seek uint64) {
	// k should always be -1 unless we're seeking into a run container. Then the
	// "if c.isRun" section will take care of it.
	itr.k = -1

	// Move to the correct container.
	itr.citer, _ = itr.bitmap.Containers.Iterator(highbits(seek))
	if !itr.citer.Next() {
		itr.c = nil
		return // eof
	}
	itr.key, itr.c = itr.citer.Value()

	// Move to the correct value index inside the container.
	lb := lowbits(seek)
	if itr.c.isArray() {
		// Find index in the container.
		itr.j = search32(itr.c.array, lb)
		if itr.j < 0 {
			itr.j = -itr.j - 1
		}
		if itr.j < len(itr.c.array) {
			itr.j--
			return
		}

		// If it's at the end of the container then move to the next one.
		if !itr.citer.Next() {
			itr.c = nil
			return
		}
		itr.key, itr.c = itr.citer.Value()
		itr.j = -1
		return
	}

	if itr.c.isRun() {
		if seek == 0 {
			itr.j, itr.k = 0, -1
		}

		j, contains := binSearchRuns(lb, itr.c.runs)
		if contains {
			itr.j = j
			itr.k = int(lb) - int(itr.c.runs[j].start) - 1
		} else {
			// Set iterator to next value in the Bitmap.
			itr.j = j
			itr.k = -1
		}

		return
	}

	// If it's a bitmap container then move to index before the value and call next().
	itr.j = int(lb) - 1
}

// Next returns the next value in the bitmap.
// Returns eof as true if there are no values left in the iterator.
func (itr *Iterator) Next() (v uint64, eof bool) {
	if itr.c == nil {
		return 0, true
	}
	// Iterate over containers until we find the next value or EOF.
	for {
		if itr.c.isArray() {
			if itr.j >= itr.c.n-1 {
				// Reached end of array, move to the next container.
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}
			itr.j++
			return itr.peek(), false
		}

		if itr.c.isRun() {
			// Because itr.j for an array container defaults to -1
			// but defaults to 0 for a run container, we need to
			// standardize on treating -1 as our default value for itr.j.
			// Note that this is easier than changing the default to 0
			// because the array logic uses the negative number space
			// to represent offsets to an array position that isn't filled
			// (-1 being the first empty space in an array, or 0).
			if itr.j == -1 {
				itr.j++
			}

			// If the container is empty, move to the next container.
			if len(itr.c.runs) == 0 {
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}

			r := itr.c.runs[itr.j]
			runLength := int(r.last - r.start)

			if itr.k >= runLength {
				// Reached end of run, move to the next run.
				itr.j, itr.k = itr.j+1, -1
			}

			if itr.j >= len(itr.c.runs) {
				// Reached end of runs, move to the next container.
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}

			itr.k++
			return itr.peek(), false
		}

		// Move to the next possible index in the bitmap container.
		itr.j++

		// Find first non-zero bit in current bitmap, if possible.
		hb := itr.j >> 6

		if hb >= len(itr.c.bitmap) {
			if !itr.citer.Next() {
				itr.c = nil
				return 0, true
			}
			itr.key, itr.c = itr.citer.Value()
			itr.j = -1
			continue
		}
		lb := itr.c.bitmap[hb] >> (uint(itr.j) % 64)
		if lb != 0 {
			itr.j = itr.j + trailingZeroN(lb)
			return itr.peek(), false
		}

		// Otherwise iterate through remaining bitmaps to find next bit.
		for hb++; hb < len(itr.c.bitmap); hb++ {
			if itr.c.bitmap[hb] != 0 {
				itr.j = hb<<6 + trailingZeroN(itr.c.bitmap[hb])
				return itr.peek(), false
			}
		}

		// If no bits found then move to the next container.
		if !itr.citer.Next() {
			itr.c = nil
			return 0, true
		}
		itr.key, itr.c = itr.citer.Value()
		itr.j = -1
	}
}

// peek returns the current value.
func (itr *Iterator) peek() uint64 {
	if itr.c == nil {
		return 0
	}
	if itr.c.isArray() {
		return itr.key<<16 | uint64(itr.c.array[itr.j])
	}
	if itr.c.isRun() {
		return itr.key<<16 | uint64(itr.c.runs[itr.j].start+uint16(itr.k))
	}
	return itr.key<<16 | uint64(itr.j)
}

// ArrayMaxSize represents the maximum size of array containers.
const ArrayMaxSize = 4096

// runMaxSize represents the maximum size of run length encoded containers.
const runMaxSize = 2048

// Container represents a Container for uint16 integers.
//
// These are used for storing the low bits of numbers in larger sets of uint64.
// The high bits are stored in a Container's key which is tracked by a separate
// data structure. Integers in a Container can be encoded in one of three ways -
// the encoding used is usually whichever is most compact, though any Container
// type should be able to encode any set of integers safely. For containers with
// less than 4,096 values, an array is often used. Containers with long runs of
// integers would use run length encoding, and more random data usually uses
// bitmap encoding.
type Container struct {
	mapped        bool         // mapped directly to a byte slice when true
	containerType byte         // array, bitmap, or run
	n             int          // number of integers in container
	array         []uint16     // used for array containers
	bitmap        []uint64     // used for bitmap containers
	runs          []interval16 // used for RLE containers
}

type interval16 struct {
	start uint16
	last  uint16
}

// runlen returns the count of integers in the interval.
func (iv interval16) runlen() int {
	return 1 + int(iv.last-iv.start)
}

// newContainer returns a new instance of container.
func NewContainer() *Container {
	return &Container{containerType: containerArray}
}

// Mapped returns true if the container is mapped directly to a byte slice
func (c *Container) Mapped() bool {
	return c.mapped
}

// N returns the cached bit count of the container
func (c *Container) N() int {
	return c.n
}

// Update updates the container
func (c *Container) Update(containerType byte, n int, mapped bool) {
	c.containerType = containerType
	c.n = n
	c.mapped = mapped
}

// isArray returns true if the container is an array container.
func (c *Container) isArray() bool {
	return c.containerType == containerArray
}

// isBitmap returns true if the container is a bitmap container.
func (c *Container) isBitmap() bool {
	return c.containerType == containerBitmap
}

// isRun returns true if the container is a run-length-encoded container.
func (c *Container) isRun() bool {
	return c.containerType == containerRun
}

// unmap creates copies of the containers data in the heap.
//
// This is performed when altering the container since its contents could be
// pointing at a read-only mmap.
func (c *Container) unmap() {
	if !c.mapped {
		return
	}

	switch c.containerType {
	case containerArray:
		tmp := make([]uint16, len(c.array))
		copy(tmp, c.array)
		c.array = tmp
	case containerBitmap:
		tmp := make([]uint64, len(c.bitmap))
		copy(tmp, c.bitmap)
		c.bitmap = tmp
	case containerRun:
		tmp := make([]interval16, len(c.runs))
		copy(tmp, c.runs)
		c.runs = tmp
	}
	c.mapped = false
}

// count counts all bits in the container.
func (c *Container) count() (n int) {
	return c.countRange(0, maxContainerVal+1)
}

// countRange counts the number of bits set between [start, end).
func (c *Container) countRange(start, end int) (n int) {
	if c.isArray() {
		return c.arrayCountRange(start, end)
	} else if c.isRun() {
		return c.runCountRange(start, end)
	}
	return c.bitmapCountRange(start, end)
}

func (c *Container) arrayCountRange(start, end int) (n int) {
	i := sort.Search(len(c.array), func(i int) bool { return int(c.array[i]) >= start })
	for ; i < len(c.array); i++ {
		v := int(c.array[i])
		if v >= end {
			break
		}
		n++
	}
	return n
}

func (c *Container) bitmapCountRange(start, end int) int {
	var n uint64
	i, j := start/64, end/64
	// Special case when start and end fall in the same word.
	if i == j {
		offi, offj := uint(start%64), uint(64-end%64)
		n += popcount((c.bitmap[i] >> offi) << (offj + offi))
		return int(n)
	}

	// Count partial starting word.
	if off := uint(start) % 64; off != 0 {
		n += popcount(c.bitmap[i] >> off)
		i++
	}

	// Count words in between.
	for ; i < j; i++ {
		n += popcount(c.bitmap[i])
	}

	// Count partial ending word.
	if j < len(c.bitmap) {
		off := 64 - (uint(end) % 64)
		n += popcount(c.bitmap[j] << off)
	}

	return int(n)
}

func (c *Container) runCountRange(start, end int) (n int) {
	for _, iv := range c.runs {
		// iv is before range
		if int(iv.last) < start {
			continue
		}
		// iv is after range
		if end < int(iv.start) {
			break
		}
		// iv is superset of range
		if int(iv.start) < start && int(iv.last) > end {
			return end - start
		}
		// iv is subset of range
		if int(iv.start) >= start && int(iv.last) < end {
			n += iv.runlen()
		}
		// iv overlaps beginning of range
		if int(iv.start) < start && int(iv.last) < end {
			n += int(iv.last) - start + 1
		}
		// iv overlaps end of range
		if int(iv.start) > start && int(iv.last) >= end {
			n += end - int(iv.start)
		}
	}
	return n
}

// add adds a value to the container.
func (c *Container) add(v uint16) (added bool) {

	if c.isArray() {
		added = c.arrayAdd(v)
	} else if c.isRun() {
		added = c.runAdd(v)
	} else {
		added = c.bitmapAdd(v)
	}
	if added {
		c.n++
	}
	return added
}

func (c *Container) arrayAdd(v uint16) bool {
	// Optimize appending to the end of an array container.
	if c.n > 0 && c.n < ArrayMaxSize && c.isArray() && c.array[c.n-1] < v {
		c.unmap()
		c.array = append(c.array, v)
		return true
	}

	// Find index of the integer in the container. Exit if it already exists.
	i := search32(c.array, v)
	if i >= 0 {
		return false
	}

	// Convert to a bitmap container if too many values are in an array container.
	if c.n >= ArrayMaxSize {
		c.arrayToBitmap()
		return c.bitmapAdd(v)
	}

	// Otherwise insert into array.
	c.unmap()
	i = -i - 1
	c.array = append(c.array, 0)
	copy(c.array[i+1:], c.array[i:])
	c.array[i] = v
	return true

}

func (c *Container) bitmapAdd(v uint16) bool {
	if c.bitmapContains(v) {
		return false
	}
	c.unmap()
	c.bitmap[v/64] |= (1 << uint64(v%64))
	return true
}

func (c *Container) runAdd(v uint16) bool {
	if len(c.runs) == 0 {
		c.unmap()
		c.runs = []interval16{{start: v, last: v}}
		return true
	}

	i := sort.Search(len(c.runs),
		func(i int) bool { return c.runs[i].last >= v })

	if i == len(c.runs) {
		i--
	}

	iv := c.runs[i]
	if v >= iv.start && iv.last >= v {
		return false
	}

	c.unmap()
	if iv.last < v {
		if iv.last == v-1 {
			c.runs[i].last++
		} else {
			c.runs = append(c.runs, interval16{start: v, last: v})
		}
	} else if v+1 == iv.start {
		// combining two intervals
		if i > 0 && c.runs[i-1].last == v-1 {
			c.runs[i-1].last = iv.last
			c.runs = append(c.runs[:i], c.runs[i+1:]...)
			return true
		}
		// just before an interval
		c.runs[i].start--
	} else if i > 0 && v-1 == c.runs[i-1].last {
		// just after an interval
		c.runs[i-1].last++
	} else {
		// alone
		newIv := interval16{start: v, last: v}
		c.runs = append(c.runs[:i], append([]interval16{newIv}, c.runs[i:]...)...)
	}
	return true
}

// contains returns true if v is in the container.
func (c *Container) contains(v uint16) bool {
	if c.isArray() {
		return c.arrayContains(v)
	} else if c.isRun() {
		return c.runContains(v)
	} else {
		return c.bitmapContains(v)
	}
}

func (c *Container) bitmapCountRuns() (r int) {
	for i := 0; i < 1023; i++ {
		v, v1 := c.bitmap[i], c.bitmap[i+1]
		r = r + int(popcount((v<<1)&^v)+((v>>63)&^v1))
	}
	vl := c.bitmap[len(c.bitmap)-1]
	r = r + int(popcount((vl<<1)&^vl)+vl>>63)
	return r
}

func (c *Container) arrayCountRuns() (r int) {
	prev := -2
	for _, v := range c.array {
		if prev+1 != int(v) {
			r++
		}
		prev = int(v)
	}
	return r
}

func (c *Container) countRuns() (r int) {
	if c.isArray() {
		return c.arrayCountRuns()
	} else if c.isBitmap() {
		return c.bitmapCountRuns()
	} else if c.isRun() {
		return len(c.runs)
	}

	// sure hope this never happens
	return 0
}

// optimize converts the container to the type which will take up the least
// amount of space.
func (c *Container) optimize() {
	if c.n == 0 {
		return
	}
	runs := c.countRuns()

	var newType byte
	if runs <= runMaxSize && runs <= c.n/2 {
		newType = containerRun
	} else if c.n < ArrayMaxSize {
		newType = containerArray
	} else {
		newType = containerBitmap
	}

	// Then convert accordingly.
	if c.isArray() {
		if newType == containerBitmap {
			c.arrayToBitmap()
		} else if newType == containerRun {
			c.arrayToRun()
		}
	} else if c.isBitmap() {
		if newType == containerArray {
			c.bitmapToArray()
		} else if newType == containerRun {
			c.bitmapToRun()
		}
	} else if c.isRun() {
		if newType == containerBitmap {
			c.runToBitmap()
		} else if newType == containerArray {
			c.runToArray()
		}
	}
}

func (c *Container) arrayContains(v uint16) bool {
	return search32(c.array, v) >= 0
}

func (c *Container) bitmapContains(v uint16) bool {
	return (c.bitmap[v/64] & (1 << uint64(v%64))) != 0
}

// binSearchRuns returns the index of the run containing v, and true, when v is contained;
// or the index of the next run starting after v, and false, when v is not contained.
func binSearchRuns(v uint16, a []interval16) (int, bool) {
	i := sort.Search(len(a),
		func(i int) bool { return a[i].last >= v })
	if i < len(a) {
		return i, (v >= a[i].start) && (v <= a[i].last)
	}

	return i, false
}

// runContains determines if v is in the container assuming c is a run
// container.
func (c *Container) runContains(v uint16) bool {
	_, found := binSearchRuns(v, c.runs)
	return found
}

// remove removes a value from the container.
func (c *Container) remove(v uint16) (removed bool) {
	if c.isArray() {
		removed = c.arrayRemove(v)
	} else if c.isRun() {
		removed = c.runRemove(v)
	} else {
		removed = c.bitmapRemove(v)
	}
	if removed {
		c.n--
	}
	return removed
}

func (c *Container) arrayRemove(v uint16) bool {
	i := search32(c.array, v)
	if i < 0 {
		return false
	}
	c.unmap()

	c.array = append(c.array[:i], c.array[i+1:]...)
	return true
}

func (c *Container) bitmapRemove(v uint16) bool {
	if !c.bitmapContains(v) {
		return false
	}
	c.unmap()

	// Lower count and remove element.
	// c.n-- // TODO removed this - test it
	c.bitmap[v/64] &^= (uint64(1) << uint(v%64))

	// Convert to array if we go below the threshold.
	if c.n == ArrayMaxSize {
		c.bitmapToArray()
	}
	return true
}

// runRemove removes v from a run container, and returns true if v was removed.
func (c *Container) runRemove(v uint16) bool {
	i, contains := binSearchRuns(v, c.runs)
	if !contains {
		return false
	}
	c.unmap()
	if v == c.runs[i].last && v == c.runs[i].start {
		c.runs = append(c.runs[:i], c.runs[i+1:]...)
	} else if v == c.runs[i].last {
		c.runs[i].last--
	} else if v == c.runs[i].start {
		c.runs[i].start++
	} else if v > c.runs[i].start {
		last := c.runs[i].last
		c.runs[i].last = v - 1
		c.runs = append(c.runs[:i+1], append([]interval16{{start: v + 1, last: last}}, c.runs[i+1:]...)...)
	}
	return true
}

// max returns the maximum value in the container.
func (c *Container) max() uint16 {
	if c.isArray() {
		return c.arrayMax()
	} else if c.isRun() {
		return c.runMax()
	} else {
		return c.bitmapMax()
	}
}

func (c *Container) arrayMax() uint16 {
	if len(c.array) == 0 {
		return 0 // probably hiding some ugly bug but it prevents a crash
	}
	return c.array[len(c.array)-1]
}

func (c *Container) bitmapMax() uint16 {
	// Search bitmap in reverse order.
	for i := len(c.bitmap); i > 0; i-- {
		// If value is zero then skip.
		v := c.bitmap[i-1]
		if v != 0 {
			r := bits.LeadingZeros64(v)
			return uint16((i-1)*64 + 63 - r)
		}

	}
	return 0
}

func (c *Container) runMax() uint16 {
	if len(c.runs) == 0 {
		return 0
	}
	return c.runs[len(c.runs)-1].last
}

// bitmapToArray converts from bitmap format to array format.
func (c *Container) bitmapToArray() {
	c.array = make([]uint16, 0, c.n)
	c.containerType = containerArray

	// return early if empty
	if c.n == 0 {
		c.bitmap = nil
		c.mapped = false
		return
	}

	for i, bitmap := range c.bitmap {
		for bitmap != 0 {
			t := bitmap & -bitmap
			c.array = append(c.array, uint16((i*64 + int(popcount(t-1)))))
			bitmap ^= t
		}
	}
	c.bitmap = nil
	c.mapped = false
}

// arrayToBitmap converts from array format to bitmap format.
func (c *Container) arrayToBitmap() {
	c.bitmap = make([]uint64, bitmapN)
	c.containerType = containerBitmap

	// return early if empty
	if c.n == 0 {
		c.array = nil
		c.mapped = false
		return
	}

	for _, v := range c.array {
		c.bitmap[int(v)/64] |= (uint64(1) << uint(v%64))
	}
	c.array = nil
	c.mapped = false
}

// runToBitmap converts from RLE format to bitmap format.
func (c *Container) runToBitmap() {
	c.bitmap = make([]uint64, bitmapN)
	c.containerType = containerBitmap

	// return early if empty
	if c.n == 0 {
		c.runs = nil
		c.mapped = false
		return
	}

	for _, r := range c.runs {
		// TODO this can be ~64x faster for long runs by setting maxBitmap instead of single bits
		//note v must be int or will overflow
		for v := int(r.start); v <= int(r.last); v++ {
			c.bitmap[v/64] |= (uint64(1) << uint(v%64))
		}
	}
	c.runs = nil
	c.mapped = false
}

// bitmapToRun converts from bitmap format to RLE format.
func (c *Container) bitmapToRun() {
	c.containerType = containerRun
	// return early if empty
	if c.n == 0 {
		c.runs = make([]interval16, 0)
		c.bitmap = nil
		c.mapped = false
		return
	}

	numRuns := c.bitmapCountRuns()
	c.runs = make([]interval16, 0, numRuns)

	current := c.bitmap[0]
	var i, start, last uint16
	for {
		// skip while empty
		for current == 0 && i < bitmapN-1 {
			i++
			current = c.bitmap[i]
		}

		if current == 0 {
			break
		}
		currentStart := uint16(trailingZeroN(current))
		start = 64*i + currentStart

		// pad LSBs with 1s
		current = current | (current - 1)

		// find next 0
		for current == maxBitmap && i < bitmapN-1 {
			i++
			current = c.bitmap[i]
		}

		if current == maxBitmap {

			// bitmap[1023] == maxBitmap
			c.runs = append(c.runs, interval16{start, maxContainerVal})
			break
		}
		currentLast := uint16(trailingZeroN(^current))
		last = 64*i + currentLast
		c.runs = append(c.runs, interval16{start, last - 1})

		// pad LSBs with 0s
		current = current & (current + 1)
	}

	c.bitmap = nil
	c.mapped = false
}

// arrayToRun converts from array format to RLE format.
func (c *Container) arrayToRun() {
	c.containerType = containerRun
	// return early if empty
	if c.n == 0 {
		c.runs = make([]interval16, 0)
		c.array = nil
		c.mapped = false
		return
	}

	numRuns := c.arrayCountRuns()
	c.runs = make([]interval16, 0, numRuns)
	start := c.array[0]
	for i, v := range c.array[1:] {
		if v-c.array[i] > 1 {
			// if current-previous > 1, one run ends and another begins
			c.runs = append(c.runs, interval16{start, c.array[i]})
			start = v
		}
	}
	// append final run
	c.runs = append(c.runs, interval16{start, c.array[c.n-1]})
	c.array = nil
	c.mapped = false
}

// runToArray converts from RLE format to array format.
func (c *Container) runToArray() {
	c.containerType = containerArray
	c.array = make([]uint16, 0, c.n)

	// return early if empty
	if c.n == 0 {
		c.runs = nil
		c.mapped = false
		return
	}

	for _, r := range c.runs {
		for v := int(r.start); v <= int(r.last); v++ {
			c.array = append(c.array, uint16(v))
		}
	}
	c.runs = nil
	c.mapped = false
}

// Clone returns a copy of c.
func (c *Container) Clone() *Container {
	other := &Container{n: c.n, containerType: c.containerType}

	switch c.containerType {
	case containerArray:
		other.array = make([]uint16, len(c.array))
		copy(other.array, c.array)
	case containerBitmap:
		other.bitmap = make([]uint64, len(c.bitmap))
		copy(other.bitmap, c.bitmap)
	case containerRun:
		other.runs = make([]interval16, len(c.runs))
		copy(other.runs, c.runs)
	}
	return other
}

// WriteTo writes c to w.
func (c *Container) WriteTo(w io.Writer) (n int64, err error) {
	if c.isArray() {
		return c.arrayWriteTo(w)
	} else if c.isRun() {
		return c.runWriteTo(w)
	} else {
		return c.bitmapWriteTo(w)
	}
}

func (c *Container) arrayWriteTo(w io.Writer) (n int64, err error) {
	if len(c.array) == 0 {
		return 0, nil
	}

	// Verify all elements are valid.
	// TODO: instead of commenting this out, we need to make it a configuration option
	//	for _, v := range c.array {
	//	assert(lowbits(uint64(v)) == v, "cannot write array value out of range: %d", v)
	//}

	// Write sizeof(uint32) * cardinality bytes.
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.array[0]))[:2*c.n])
	return int64(nn), err
}

func (c *Container) bitmapWriteTo(w io.Writer) (n int64, err error) {
	// Write sizeof(uint64) * bitmapN bytes.
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.bitmap[0]))[:(8 * bitmapN)])
	return int64(nn), err
}

func (c *Container) runWriteTo(w io.Writer) (n int64, err error) {
	if len(c.runs) == 0 {
		return 0, nil
	}
	var byte2 [2]byte
	binary.LittleEndian.PutUint16(byte2[:], uint16(len(c.runs)))
	_, err = w.Write(byte2[:])
	if err != nil {
		return 0, err
	}
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.runs[0]))[:interval16Size*len(c.runs)])
	return int64(runCountHeaderSize + nn), err
}

// size returns the encoded size of the container, in bytes.
func (c *Container) size() int {
	if c.isArray() {
		return len(c.array) * 2 // sizeof(uint16)
	} else if c.isRun() {
		return len(c.runs)*interval16Size + runCountHeaderSize
	} else {
		return len(c.bitmap) * 8 // sizeof(uint64)
	}
}

// info returns the current stats about the container.
func (c *Container) info() containerInfo {
	info := containerInfo{N: c.n}

	if c.isArray() {
		info.Type = "array"
		info.Alloc = len(c.array) * 2 // sizeof(uint16)
	} else if c.isRun() {
		info.Type = "run"
		info.Alloc = len(c.runs)*interval16Size + runCountHeaderSize
	} else {
		info.Type = "bitmap"
		info.Alloc = len(c.bitmap) * 8 // sizeof(uint64)
	}

	if c.mapped {
		if c.isArray() {
			info.Pointer = unsafe.Pointer(&c.array[0])
		} else if c.isRun() {
			info.Pointer = unsafe.Pointer(&c.runs[0])
		} else {
			info.Pointer = unsafe.Pointer(&c.bitmap[0])
		}
	}

	return info
}

// check performs a consistency check on the container.
func (c *Container) check() error {
	var a ErrorList

	if c.isArray() {
		if len(c.array) != c.n {
			a.Append(fmt.Errorf("array count mismatch: count=%d, n=%d", len(c.array), c.n))
		}
	} else if c.isRun() {
		n := c.runCountRange(0, maxContainerVal+1)
		if n != c.n {
			a.Append(fmt.Errorf("run count mismatch: count=%d, n=%d", n, c.n))
		}
	} else if c.isBitmap() {
		if n := c.bitmapCountRange(0, maxContainerVal+1); n != c.n {
			a.Append(fmt.Errorf("bitmap count mismatch: count=%d, n=%d", n, c.n))
		}
	} else {
		a.Append(fmt.Errorf("empty container"))
		if c.n != 0 {
			a.Append(fmt.Errorf("empty container with nonzero count: n=%d", c.n))
		}
	}

	if a == nil {
		return nil
	}
	return a
}

// containerInfo represents a point-in-time snapshot of container stats.
type containerInfo struct {
	Key     uint64         // container key
	Type    string         // container type (array, bitmap, or run)
	N       int            // number of bits
	Alloc   int            // memory used
	Pointer unsafe.Pointer // offset within the mmap
}

// flip returns a new container containing the inverse of all
// bits in a.
func flip(a *Container) *Container {
	if a.isArray() {
		return flipArray(a)
	} else if a.isRun() {
		return flipRun(a)
	} else {
		return flipBitmap(a)
	}
}

func flipArray(b *Container) *Container {
	// TODO: actually implement this
	x := b.Clone()
	x.arrayToBitmap()
	return flipBitmap(x)
}

func flipBitmap(b *Container) *Container {
	other := &Container{bitmap: make([]uint64, bitmapN), containerType: containerBitmap}

	for i, bitmap := range b.bitmap {
		other.bitmap[i] = ^bitmap
	}

	other.n = other.count()
	return other
}

func flipRun(b *Container) *Container {
	// TODO: actually implement this
	x := b.Clone()
	x.runToBitmap()
	return flipBitmap(x)
}

func intersectionCount(a, b *Container) int {
	if a.isArray() {
		if b.isArray() {
			return intersectionCountArrayArray(a, b)
		} else if b.isRun() {
			return intersectionCountArrayRun(a, b)
		} else {
			return intersectionCountArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectionCountArrayRun(b, a)
		} else if b.isRun() {
			return intersectionCountRunRun(a, b)
		} else {
			return intersectionCountBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return intersectionCountArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectionCountBitmapRun(a, b)
		} else {
			return intersectionCountBitmapBitmap(a, b)
		}
	}
}

func intersectionCountArrayArray(a, b *Container) (n int) {
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.array[i], b.array[j]
		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			n++
			i, j = i+1, j+1
		}
	}
	return n
}

func intersectionCountArrayRun(a, b *Container) (n int) {
	na, nb := len(a.array), len(b.runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.array[i], b.runs[j]
		if va < vb.start {
			i++
		} else if va >= vb.start && va <= vb.last {
			i++
			n++
		} else if va > vb.last {
			j++
		}
	}
	return n
}

func intersectionCountRunRun(a, b *Container) (n int) {
	na, nb := len(a.runs), len(b.runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.runs[i], b.runs[j]
		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if va.start > vb.last {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			n += 1 + int(vb.last-va.start)
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			n += 1 + int(vb.last-vb.start)
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			n += 1 + int(va.last-va.start)
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			n += 1 + int(va.last-vb.start)
			i++
		}
	}
	return
}

func intersectionCountBitmapRun(a, b *Container) (n int) {
	for _, iv := range b.runs {
		n += a.bitmapCountRange(int(iv.start), int(iv.last)+1)
	}
	return n
}

func intersectionCountArrayBitmap(a, b *Container) (n int) {
	ln := len(b.bitmap)
	for _, val := range a.array {
		i := int(val >> 6)
		if i >= ln {
			break
		}
		off := val % 64
		n += int(b.bitmap[i]>>off) & 1
	}
	return n
}

func intersectionCountBitmapBitmap(a, b *Container) (n int) {
	return int(popcountAndSlice(a.bitmap, b.bitmap))
}

func intersect(a, b *Container) *Container {
	if a.isArray() {
		if b.isArray() {
			return intersectArrayArray(a, b)
		} else if b.isRun() {
			return intersectArrayRun(a, b)
		} else {
			return intersectArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectArrayRun(b, a)
		} else if b.isRun() {
			return intersectRunRun(a, b)
		} else {
			return intersectBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return intersectArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectBitmapRun(a, b)
		} else {
			return intersectBitmapBitmap(a, b)
		}
	}
}

func intersectArrayArray(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.array[i], b.array[j]
		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			output.array = append(output.array, va)
			i, j = i+1, j+1
		}
	}
	output.n = len(output.array)
	return output
}

// intersectArrayRun computes the intersect of an array container and a run
// container. The return is always an array container (since it's guaranteed to
// be low-cardinality)
func intersectArrayRun(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	na, nb := len(a.array), len(b.runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.array[i], b.runs[j]
		if va < vb.start {
			i++
		} else if va > vb.last {
			j++
		} else {
			output.array = append(output.array, va)
			i++
		}
	}
	output.n = len(output.array)
	return output
}

// intersectRunRun computes the intersect of two run containers.
func intersectRunRun(a, b *Container) *Container {
	output := &Container{containerType: containerRun}
	na, nb := len(a.runs), len(b.runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.runs[i], b.runs[j]
		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if vb.last < va.start {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			output.n += output.runAppendInterval(interval16{start: va.start, last: vb.last})
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			output.n += output.runAppendInterval(vb)
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			output.n += output.runAppendInterval(va)
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			output.n += output.runAppendInterval(interval16{start: vb.start, last: va.last})
			i++
		}
	}
	if output.n < ArrayMaxSize && len(output.runs) > output.n/2 {
		output.runToArray()
	} else if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// intersectBitmapRun returns an array container if the run container's
// cardinality is < ArrayMaxSize. Otherwise it returns a bitmap container.
func intersectBitmapRun(a, b *Container) *Container {
	var output *Container
	if b.n < ArrayMaxSize {
		// output is array container
		output = &Container{containerType: containerArray}
		for _, iv := range b.runs {
			for i := iv.start; i <= iv.last; i++ {
				if a.bitmapContains(i) {
					output.array = append(output.array, i)
				}
				// If the run ends the container, break to avoid an infinite loop.
				if i == 65535 {
					break
				}
			}
		}
		output.n = len(output.array)
	} else {
		// right now this iterates through the runs and sets integers in the
		// bitmap that are in the runs. alternately, we could zero out ranges in
		// the bitmap which are between runs.
		output = &Container{
			bitmap:        make([]uint64, bitmapN),
			containerType: containerBitmap,
		}
		for j := 0; j < len(b.runs); j++ {
			vb := b.runs[j]
			i := vb.start >> 6 // index into a
			vastart := i << 6
			valast := vastart + 63
			for valast >= vb.start && vastart <= vb.last && i < bitmapN {
				if vastart >= vb.start && valast <= vb.last { // a within b
					output.bitmap[i] = a.bitmap[i]
					output.n += int(popcount(a.bitmap[i]))
				} else if vb.start >= vastart && vb.last <= valast { // b within a
					var mask uint64 = ((1 << (vb.last - vb.start + 1)) - 1) << (vb.start - vastart)
					bits := a.bitmap[i] & mask
					output.bitmap[i] |= bits
					output.n += int(popcount(bits))
				} else if vastart < vb.start { // a overlaps front of b
					offset := 64 - (1 + valast - vb.start)
					bits := (a.bitmap[i] >> offset) << offset
					output.bitmap[i] |= bits
					output.n += int(popcount(bits))
				} else if vb.start < vastart { // b overlaps front of a
					offset := 64 - (1 + vb.last - vastart)
					bits := (a.bitmap[i] << offset) >> offset
					output.bitmap[i] |= bits
					output.n += int(popcount(bits))
				}
				// update loop vars
				i++
				vastart = i << 6
				valast = vastart + 63
			}
		}
		if output.n < ArrayMaxSize {
			output.bitmapToArray()
		}
	}
	return output
}

func intersectArrayBitmap(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	for _, va := range a.array {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := b.bitmap[bmidx]
		if b&mask > 0 {
			output.array = append(output.array, va)
		}
	}
	output.n = len(output.array)
	return output
}

func intersectBitmapBitmap(a, b *Container) *Container {
	output := &Container{bitmap: make([]uint64, bitmapN), containerType: containerBitmap}

	for i := range a.bitmap {
		v := a.bitmap[i] & b.bitmap[i]
		output.bitmap[i] = v
		output.n += int(popcount(v))

	}
	output.optimize()
	return output
}

func union(a, b *Container) *Container {
	if a.isArray() {
		if b.isArray() {
			return unionArrayArray(a, b)
		} else if b.isRun() {
			return unionArrayRun(a, b)
		} else {
			return unionArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return unionArrayRun(b, a)
		} else if b.isRun() {
			return unionRunRun(a, b)
		} else {
			return unionBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return unionArrayBitmap(b, a)
		} else if b.isRun() {
			return unionBitmapRun(a, b)
		} else {
			return unionBitmapBitmap(a, b)
		}
	}
}

func unionArrayArray(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; ; {
		if i >= na && j >= nb {
			break
		} else if i < na && j >= nb {
			output.add(a.array[i])
			i++
			continue
		} else if i >= na && j < nb {
			output.add(b.array[j])
			j++
			continue
		}

		va, vb := a.array[i], b.array[j]
		if va < vb {
			output.add(va)
			i++
		} else if va > vb {
			output.add(vb)
			j++
		} else {
			output.add(va)
			i, j = i+1, j+1
		}
	}
	return output
}

// unionArrayRun optimistically assumes that the result will be a run container,
// and converts to a bitmap or array container afterwards if necessary.
func unionArrayRun(a, b *Container) *Container {
	if b.n == maxContainerVal+1 {
		return b.Clone()
	}
	output := &Container{containerType: containerRun}
	na, nb := len(a.array), len(b.runs)
	var vb interval16
	var va uint16
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = a.array[i]
		}
		if j < nb {
			vb = b.runs[j]
		}
		if i < na && (j >= nb || va < vb.start) {
			output.n += output.runAppendInterval(interval16{start: va, last: va})
			i++
		} else {
			output.n += output.runAppendInterval(vb)
			j++
		}
	}
	if output.n < ArrayMaxSize {
		output.runToArray()
	} else if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// runAppendInterval adds the given interval to the run container. It assumes
// that the interval comes at the end of the list of runs, and does not check
// that this is the case. It will not behave correctly if the start of the given
// interval is earlier than the start of the last interval in the list of runs.
// Its return value is the amount by which the cardinality of the container was
// increased.
func (c *Container) runAppendInterval(v interval16) int {
	if len(c.runs) == 0 {
		c.runs = append(c.runs, v)
		return int(v.last-v.start) + 1
	}

	last := c.runs[len(c.runs)-1]
	if last.last == maxContainerVal { //protect against overflow
		return 0
	}
	if last.last+1 >= v.start && v.last > last.last {
		c.runs[len(c.runs)-1].last = v.last
		return int(v.last - last.last)
	} else if last.last+1 < v.start {
		c.runs = append(c.runs, v)
		return int(v.last-v.start) + 1
	}
	return 0
}

func unionRunRun(a, b *Container) *Container {
	if a.n == maxContainerVal+1 {
		return a.Clone()
	}
	if b.n == maxContainerVal+1 {
		return b.Clone()
	}
	na, nb := len(a.runs), len(b.runs)
	output := &Container{
		runs:          make([]interval16, 0, na+nb),
		containerType: containerRun,
	}
	var va, vb interval16
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = a.runs[i]
		}
		if j < nb {
			vb = b.runs[j]
		}
		if i < na && (j >= nb || va.start < vb.start) {
			output.n += output.runAppendInterval(va)
			i++
		} else {
			output.n += output.runAppendInterval(vb)
			j++
		}
	}
	if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

func unionBitmapRun(a, b *Container) *Container {
	if b.n == maxContainerVal+1 {
		return b.Clone()
	}
	if a.n == maxContainerVal+1 {
		return a.Clone()
	}
	output := a.Clone()
	for j := 0; j < len(b.runs); j++ {
		output.bitmapSetRange(uint64(b.runs[j].start), uint64(b.runs[j].last)+1)
	}
	return output
}

const maxBitmap = 0xFFFFFFFFFFFFFFFF

// sets all bits in [i, j) (c must be a bitmap container)
func (c *Container) bitmapSetRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	xcnt := popcount(X)
	ycnt := popcount(Y)
	if x == y {
		c.n += int((j - i) - popcount(c.bitmap[x]&(X&Y)))
		c.bitmap[x] |= (X & Y)
	} else {
		c.n += int(xcnt - popcount(c.bitmap[x]&X))
		c.bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			c.n += int(64 - popcount(c.bitmap[i]))
			c.bitmap[i] = maxBitmap
		}
		c.n += int(ycnt - popcount(c.bitmap[y]&Y))
		c.bitmap[y] |= Y
	}
}

// xor's all bits in [i, j) with all true (c must be a bitmap container).
func (c *Container) bitmapXorRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	if x == y {
		cnt := popcount(c.bitmap[x])
		c.bitmap[x] ^= (X & Y) //// flip
		c.n += int(popcount(c.bitmap[x]) - cnt)
	} else {
		cnt := popcount(c.bitmap[x])
		c.bitmap[x] ^= X
		c.n += int(popcount(c.bitmap[x]) - cnt)
		for i := x + 1; i < y; i++ {
			cnt = popcount(c.bitmap[i])
			c.bitmap[i] ^= maxBitmap
			c.n += int(popcount(c.bitmap[i]) - cnt)
		}
		cnt = popcount(c.bitmap[y])
		c.bitmap[y] ^= Y
		c.n += int(popcount(c.bitmap[y]) - cnt)
	}
}

// zeroes all bits in [i, j) (c must be a bitmap container)
func (c *Container) bitmapZeroRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	if x == y {
		c.n -= int(popcount(c.bitmap[x] & (X & Y)))
		c.bitmap[x] &= ^(X & Y)
	} else {
		c.n -= int(popcount(c.bitmap[x] & X))
		c.bitmap[x] &= ^X
		for i := x + 1; i < y; i++ {
			c.n -= int(popcount(c.bitmap[i]))
			c.bitmap[i] = 0
		}
		c.n -= int(popcount(c.bitmap[y] & Y))
		c.bitmap[y] &= ^Y
	}
}

func (c *Container) equals(c2 *Container) bool {
	if c.mapped != c2.mapped || c.containerType != c2.containerType || c.n != c2.n {
		return false
	}
	if c.containerType == containerArray {
		if len(c.array) != len(c2.array) {
			return false
		}
		for i := 0; i < len(c.array); i++ {
			if c.array[i] != c2.array[i] {
				return false
			}
		}
	} else if c.containerType == containerBitmap {
		if len(c.bitmap) != len(c2.bitmap) {
			return false
		}
		for i := 0; i < len(c.bitmap); i++ {
			if c.bitmap[i] != c2.bitmap[i] {
				return false
			}
		}
	} else if c.containerType == containerRun {
		if len(c.runs) != len(c2.runs) {
			return false
		}
		for i := 0; i < len(c.runs); i++ {
			if c.runs[i] != c2.runs[i] {
				return false
			}
		}
	} else {
		panic(fmt.Sprintf("unknown container type: %v", c.containerType))
	}
	return true
}

func unionArrayBitmap(a, b *Container) *Container {
	output := b.Clone()
	for _, v := range a.array {
		if !output.bitmapContains(v) {
			output.bitmap[v/64] |= (1 << uint64(v%64))
			output.n++
		}
	}
	return output
}

func unionBitmapBitmap(a, b *Container) *Container {
	output := &Container{
		bitmap:        make([]uint64, bitmapN),
		containerType: containerBitmap,
	}

	for i := 0; i < bitmapN; i++ {
		v := a.bitmap[i] | b.bitmap[i]
		output.bitmap[i] = v
		output.n += int(popcount(v))
	}

	return output
}

func difference(a, b *Container) *Container {
	if a.isArray() {
		if b.isArray() {
			return differenceArrayArray(a, b)
		} else if b.isRun() {
			return differenceArrayRun(a, b)
		} else {
			return differenceArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return differenceRunArray(a, b)
		} else if b.isRun() {
			return differenceRunRun(a, b)
		} else {
			return differenceRunBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return differenceBitmapArray(a, b)
		} else if b.isRun() {
			return differenceBitmapRun(a, b)
		} else {
			return differenceBitmapBitmap(a, b)
		}
	}
}

// differenceArrayArray computes the difference bween two arrays.
func differenceArrayArray(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na; {
		va := a.array[i]
		if j >= nb {
			output.add(va)
			i++
			continue
		}

		vb := b.array[j]
		if va < vb {
			output.add(va)
			i++
		} else if va > vb {
			j++
		} else {
			i, j = i+1, j+1
		}
	}
	return output
}

// differenceArrayRun computes the difference of an array from a run.
func differenceArrayRun(a, b *Container) *Container {
	// func (ac *arrayContainer) iandNotRun16(rc *runContainer16) container {

	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}

	output := &Container{array: make([]uint16, 0, a.n), containerType: containerArray}
	// cardinality upper bound: card(A)

	i := 0 // array index
	j := 0 // run index

	// handle overlap
	for i < a.n {

		// keep all array elements before beginning of runs
		if a.array[i] < b.runs[j].start {
			output.add(a.array[i])
			i++
			continue
		}

		// if array element in run, skip it
		if a.array[i] >= b.runs[j].start && a.array[i] <= b.runs[j].last {
			i++
			continue
		}

		// if array element larger than current run, check next run
		if a.array[i] > b.runs[j].last {
			j++
			if j == len(b.runs) {
				break
			}
		}
	}

	if i < len(a.array) {
		// keep all array elements after end of runs
		// It's possible that output was converted from array to bitmap in output.add()
		// so check container type before proceeding.
		if output.containerType == containerArray {
			output.array = append(output.array, a.array[i:]...)
			// TODO: consider handling container.n mutations in one place
			// like we do with container.add().
			output.n += len(a.array[i:])
		} else {
			for _, v := range a.array[i:] {
				output.add(v)
			}
		}
	}
	return output
}

// differenceBitmapRun computes the difference of an bitmap from a run.
func differenceBitmapRun(a, b *Container) *Container {
	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}

	output := a.Clone()
	for j := 0; j < len(b.runs); j++ {
		output.bitmapZeroRange(uint64(b.runs[j].start), uint64(b.runs[j].last)+1)
	}
	return output
}

// differenceRunArray subtracts the bits in an array container from a run
// container.
func differenceRunArray(a, b *Container) *Container {
	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}
	output := &Container{runs: make([]interval16, 0, len(a.runs)), containerType: containerRun}

	bidx := 0
	vb := b.array[bidx]

RUNLOOP:
	for _, run := range a.runs {
		start := run.start
		for vb < run.start {
			bidx++
			if bidx >= len(b.array) {
				break
			}
			vb = b.array[bidx]
		}
		for vb >= run.start && vb <= run.last {
			if vb == start {
				if vb == 65535 { // overflow
					break RUNLOOP
				}
				start++
				bidx++
				if bidx >= len(b.array) {
					break
				}
				vb = b.array[bidx]
				continue
			}
			output.runs = append(output.runs, interval16{start: start, last: vb - 1})
			output.n += int(vb - start)
			if vb == 65535 { // overflow
				break RUNLOOP
			}
			start = vb + 1
			bidx++
			if bidx >= len(b.array) {
				break
			}
			vb = b.array[bidx]
		}

		if start <= run.last {
			output.runs = append(output.runs, interval16{start: start, last: run.last})
			output.n += int(run.last - start + 1)
		}
	}
	output.optimize()
	return output
}

// differenceRunBitmap computes the difference of an run from a bitmap.
func differenceRunBitmap(a, b *Container) *Container {
	// If a is full, difference is the flip of b.
	if len(a.runs) > 0 && a.runs[0].start == 0 && a.runs[0].last == 65535 {
		return flipBitmap(b)
	}
	output := &Container{containerType: containerRun}
	output.n = a.n
	if len(a.runs) == 0 {
		return output
	}
	for j := 0; j < len(a.runs); j++ {
		run := a.runs[j]
		add := true
		for bit := a.runs[j].start; bit <= a.runs[j].last; bit++ {
			if b.bitmapContains(bit) {
				output.n--
				if run.start == bit {
					if bit == 65535 { //overflow
						add = false
					}

					run.start++
				} else if bit == run.last {
					run.last--
				} else {
					run.last = bit - 1
					if run.last >= run.start {
						output.runs = append(output.runs, run)
					}
					run.start = bit + 1
					run.last = a.runs[j].last
				}
				if run.start > run.last {
					break
				}
			}

			if bit == 65535 { //overflow
				break
			}
		}
		if run.start <= run.last {
			if add {
				output.runs = append(output.runs, run)
			}
		}
	}

	if output.n < ArrayMaxSize && len(output.runs) > output.n/2 {
		output.runToArray()
	} else if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// differenceRunRun computes the difference of two runs.
func differenceRunRun(a, b *Container) *Container {
	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}

	apos := 0 // current a-run index
	bpos := 0 // current b-run index
	astart := a.runs[apos].start
	alast := a.runs[apos].last
	bstart := b.runs[bpos].start
	blast := b.runs[bpos].last
	alen := len(a.runs)
	blen := len(b.runs)

	output := &Container{runs: make([]interval16, 0, alen+blen), containerType: containerRun} // TODO allocate max then truncate? or something else
	// cardinality upper bound: sum of number of runs
	// each B-run could split an A-run in two, up to len(b.runs) times

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// current A-run entirely precedes current B-run: keep full A-run, advance to next A-run
			output.runs = append(output.runs, interval16{start: astart, last: alast})
			apos++
			if apos < alen {
				astart = a.runs[apos].start
				alast = a.runs[apos].last
			}
		case blast < astart:
			// current B-run entirely precedes current A-run: advance to next B-run
			bpos++
			if bpos < blen {
				bstart = b.runs[bpos].start
				blast = b.runs[bpos].last
			}
		default:
			// overlap
			if astart < bstart {
				output.runs = append(output.runs, interval16{start: astart, last: bstart - 1})
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = a.runs[apos].start
					alast = a.runs[apos].last
				}
			}
		}
	}
	if apos < alen {
		output.runs = append(output.runs, interval16{start: astart, last: alast})
		apos++
		if apos < alen {
			output.runs = append(output.runs, a.runs[apos:]...)
		}
	}

	output.n = output.count()
	return output
}

func differenceArrayBitmap(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	for _, va := range a.array {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := b.bitmap[bmidx]

		if mask&^b > 0 {
			output.array = append(output.array, va)
		}
	}
	output.n = len(output.array)
	return output
}

func differenceBitmapArray(a, b *Container) *Container {
	output := a.Clone()

	for _, v := range b.array {
		if output.bitmapContains(v) {
			output.bitmap[v/64] &^= (uint64(1) << uint(v%64))
			output.n--
		}
	}
	if output.n < ArrayMaxSize {
		output.bitmapToArray()
	}
	return output
}

func differenceBitmapBitmap(a, b *Container) *Container {
	output := &Container{bitmap: make([]uint64, bitmapN), containerType: containerBitmap}

	for i := range a.bitmap {
		v := a.bitmap[i] & (^b.bitmap[i])
		output.bitmap[i] = v
		output.n += int(popcount(v))

	}
	if output.n < ArrayMaxSize {
		output.bitmapToArray()
	}
	return output
}

func xor(a, b *Container) *Container {
	if a.isArray() {
		if b.isArray() {
			return xorArrayArray(a, b)
		} else if b.isRun() {
			return xorArrayRun(a, b)
		} else {
			return xorArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return xorArrayRun(b, a)
		} else if b.isRun() {
			return xorRunRun(a, b)
		} else {
			return xorBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return xorArrayBitmap(b, a)
		} else if b.isRun() {
			return xorBitmapRun(a, b)
		} else {
			return xorBitmapBitmap(a, b)
		}
	}
}

func xorArrayArray(a, b *Container) *Container {
	output := &Container{containerType: containerArray}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && j >= nb {
			output.add(a.array[i])
			i++
			continue
		} else if i >= na && j < nb {
			output.add(b.array[j])
			j++
			continue
		}

		va, vb := a.array[i], b.array[j]
		if va < vb {
			output.add(va)
			i++
		} else if va > vb {
			output.add(vb)
			j++
		} else { //==
			i++
			j++
		}
	}
	return output
}

func xorArrayBitmap(a, b *Container) *Container {
	output := b.Clone()
	for _, v := range a.array {
		if b.bitmapContains(v) {
			output.remove(v)
		} else {
			output.add(v)
		}
	}

	// It's possible that output was converted from bitmap to array in output.remove()
	// so we only do this conversion if output is still a bitmap container.
	if output.containerType == containerBitmap && output.count() < ArrayMaxSize {
		output.bitmapToArray()
	}

	return output
}

func xorBitmapBitmap(a, b *Container) *Container {
	output := &Container{
		bitmap:        make([]uint64, bitmapN),
		containerType: containerBitmap,
	}
	for i := 0; i < bitmapN; i++ {
		v := a.bitmap[i] ^ b.bitmap[i]
		output.bitmap[i] = v
		output.n += int(popcount(v))
	}

	if output.count() < ArrayMaxSize {
		output.bitmapToArray()
	}
	return output
}

// opType represents a type of operation.
type opType uint8

const (
	opTypeAdd    = opType(0)
	opTypeRemove = opType(1)
)

// op represents an operation on the bitmap.
type op struct {
	typ   opType
	value uint64
}

// apply executes the operation against a bitmap.
func (op *op) apply(b *Bitmap) bool {
	switch op.typ {
	case opTypeAdd:
		return b.add(op.value)
	case opTypeRemove:
		return b.remove(op.value)
	default:
		panic(fmt.Sprintf("invalid op type: %d", op.typ))
	}
}

// WriteTo writes op to the w.
func (op *op) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, op.size())

	// Write type and value.
	buf[0] = byte(op.typ)
	binary.LittleEndian.PutUint64(buf[1:9], op.value)

	// Add checksum at the end.
	h := fnv.New32a()
	h.Write(buf[0:9])
	binary.LittleEndian.PutUint32(buf[9:13], h.Sum32())

	// Write to writer.
	nn, err := w.Write(buf)
	return int64(nn), err
}

// UnmarshalBinary decodes data into an op.
func (op *op) UnmarshalBinary(data []byte) error {
	if len(data) < op.size() {
		return fmt.Errorf("op data out of bounds: len=%d", len(data))
	}

	// Verify checksum.
	h := fnv.New32a()
	h.Write(data[0:9])
	if chk := binary.LittleEndian.Uint32(data[9:13]); chk != h.Sum32() {
		return fmt.Errorf("checksum mismatch: exp=%08x, got=%08x", h.Sum32(), chk)
	}

	// Read type and value.
	op.typ = opType(data[0])
	op.value = binary.LittleEndian.Uint64(data[1:9])

	return nil
}

// size returns the encoded size of the op, in bytes.
func (*op) size() int { return 1 + 8 + 4 }

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// search32 returns the index of value in a. If value is not found, it works the
// same way as search64.
func search32(a []uint16, value uint16) int {
	// Optimize for elements and the last element.
	n := len(a)
	if n == 0 {
		return -1
	} else if a[n-1] == value {
		return n - 1
	}

	// Otherwise perform binary search for exact match.
	lo, hi := 0, n-1
	for lo+16 <= hi {
		i := int(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			return i
		}
	}

	// If an exact match isn't found then return a negative index.
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			return lo
		} else if v > value {
			break
		}
	}
	return -(lo + 1)
}

// search64 returns the index of value in a. If value is not found, -1 * (1 +
// the index where v would be if it were inserted) is returned. This is done in
// order to both signal that value was not found (negative number), and also
// return information about where v would go if it were inserted. The +1 offset
// is necessary due to the case where v is not found, but would go at index 0.
// since negative 0 is no different from positive 0, we offset the returned
// negative indices by 1. See the test for this function for examples.
func search64(a []uint64, value uint64) int {
	// Optimize for elements and the last element.
	n := len(a)
	if n == 0 {
		return -1
	} else if a[n-1] == value {
		return n - 1
	}

	// Otherwise perform binary search for exact match.
	lo, hi := 0, n-1
	for lo+16 <= hi {
		i := int(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			return i
		}
	}

	// If an exact match isn't found then return a negative index.
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			return lo
		} else if v > value {
			break
		}
	}
	return -(lo + 1)
}

// trailingZeroN returns the number of trailing zeros in v.
// v must be greater than zero.
func trailingZeroN(v uint64) int {
	return bits.TrailingZeros64(v)
}

// ErrorList represents a list of errors.
type ErrorList []error

func (a ErrorList) Error() string {
	switch len(a) {
	case 0:
		return "no errors"
	case 1:
		return a[0].Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", a[0], len(a)-1)
}

// Append appends an error to the list. If err is an ErrorList then all errors are appended.
func (a *ErrorList) Append(err error) {
	switch err := err.(type) {
	case ErrorList:
		*a = append(*a, err...)
	default:
		*a = append(*a, err)
	}
}

// AppendWithPrefix appends an error to the list and includes a prefix.
func (a *ErrorList) AppendWithPrefix(err error, prefix string) {
	switch err := err.(type) {
	case ErrorList:
		for i := range err {
			*a = append(*a, fmt.Errorf("%s%s", prefix, err[i]))
		}
	default:
		*a = append(*a, fmt.Errorf("%s%s", prefix, err))
	}
}

// xorArrayRun computes the exclusive or of an array and a run container.
func xorArrayRun(a, b *Container) *Container {
	output := &Container{containerType: containerRun}
	na, nb := len(a.array), len(b.runs)
	var vb interval16
	var va uint16
	lastI, lastJ := -1, -1
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && i != lastI {
			va = a.array[i]
		}
		if j < nb && j != lastJ {
			vb = b.runs[j]
		}
		lastI = i
		lastJ = j

		if i < na && (j >= nb || va < vb.start) { //before
			output.n += output.runAppendInterval(interval16{start: va, last: va})
			i++
		} else if j < nb && (i >= na || va > vb.last) { //after
			output.n += output.runAppendInterval(vb)
			j++
		} else if va > vb.start {
			if va < vb.last {
				output.n += output.runAppendInterval(interval16{start: vb.start, last: va - 1})
				i++
				vb.start = va + 1

				if vb.start > vb.last {
					j++
				}
			} else if va > vb.last {
				output.n += output.runAppendInterval(vb)
				j++
			} else { // va == vb.last
				vb.last--
				if vb.start <= vb.last {
					output.n += output.runAppendInterval(vb)
				}
				j++
				i++
			}

		} else { // we know va == vb.start
			if vb.start == maxContainerVal { // protect overflow
				j++
			} else {
				vb.start++
				if vb.start > vb.last {
					j++
				}
			}
			i++
		}
	}
	if output.n < ArrayMaxSize {
		output.runToArray()
	} else if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// xorCompare computes first exclusive run between two runs.
func xorCompare(x *xorstm) (r1 interval16, hasData bool) {
	hasData = false
	if !x.vaValid || !x.vbValid {
		if x.vbValid {
			x.vbValid = false
			r1 = x.vb
			hasData = true
			return
		}
		if x.vaValid {
			x.vaValid = false
			r1 = x.va
			hasData = true
			return
		}
		return
	}

	if x.va.last < x.vb.start { //va  before
		x.vaValid = false
		r1 = x.va
		hasData = true
	} else if x.vb.last < x.va.start { //vb before
		x.vbValid = false
		r1 = x.vb
		hasData = true
	} else if x.va.start == x.vb.start && x.va.last == x.vb.last { // Equal
		x.vaValid = false
		x.vbValid = false
	} else if x.va.start <= x.vb.start && x.va.last >= x.vb.last { //vb inside
		x.vbValid = false
		if x.va.start != x.vb.start {
			r1 = interval16{start: x.va.start, last: x.vb.start - 1}
			hasData = true
		}

		if x.vb.last == maxContainerVal { // Check for overflow
			x.vaValid = false

		} else {
			x.va.start = x.vb.last + 1
			if x.va.start > x.va.last {
				x.vaValid = false
			}
		}

	} else if x.vb.start <= x.va.start && x.vb.last >= x.va.last { //va inside
		x.vaValid = false
		if x.vb.start != x.va.start {
			r1 = interval16{start: x.vb.start, last: x.va.start - 1}
			hasData = true
		}

		if x.va.last == maxContainerVal { //check for overflow
			x.vbValid = false
		} else {
			x.vb.start = x.va.last + 1
			if x.vb.start > x.vb.last {
				x.vbValid = false
			}
		}

	} else if x.va.start < x.vb.start && x.va.last <= x.vb.last { //va first overlap
		x.vaValid = false
		r1 = interval16{start: x.va.start, last: x.vb.start - 1}
		hasData = true
		if x.va.last == maxContainerVal { // check for overflow
			x.vbValid = false
		} else {
			x.vb.start = x.va.last + 1
			if x.vb.start > x.vb.last {
				x.vbValid = false
			}
		}
	} else if x.vb.start < x.va.start && x.vb.last <= x.va.last { //vb first overlap
		x.vbValid = false
		r1 = interval16{start: x.vb.start, last: x.va.start - 1}
		hasData = true

		if x.vb.last == maxContainerVal { // check for overflow
			x.vaValid = false
		} else {
			x.va.start = x.vb.last + 1
			if x.va.start > x.va.last {
				x.vaValid = false
			}
		}
	}
	return
}

//stm  is state machine used to "xor" iterate over runs.
type xorstm struct {
	vaValid, vbValid bool
	va, vb           interval16
}

// xorRunRun computes the exclusive or of two run containers.
func xorRunRun(a, b *Container) *Container {
	na, nb := len(a.runs), len(b.runs)
	if na == 0 {
		return b.Clone()
	}
	if nb == 0 {
		return a.Clone()
	}
	output := &Container{containerType: containerRun}

	lastI, lastJ := -1, -1

	state := &xorstm{}

	for i, j := 0, 0; i < na || j < nb; {
		if i < na && lastI != i {
			state.va = a.runs[i]
			state.vaValid = true
		}

		if j < nb && lastJ != j {
			state.vb = b.runs[j]
			state.vbValid = true
		}
		lastI, lastJ = i, j

		r1, ok := xorCompare(state)
		if ok {
			output.n += output.runAppendInterval(r1)
		}
		if !state.vaValid {
			i++
		}
		if !state.vbValid {
			j++
		}

	}

	if output.n < ArrayMaxSize && len(output.runs) > output.n/2 {
		output.runToArray()
	} else if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// xorRunRun computes the exclusive or of a bitmap and a run container.
func xorBitmapRun(a, b *Container) *Container {
	output := a.Clone()
	for j := 0; j < len(b.runs); j++ {
		output.bitmapXorRange(uint64(b.runs[j].start), uint64(b.runs[j].last)+1)
	}

	if output.n < ArrayMaxSize && len(output.runs) > output.n/2 {
		output.runToArray()
	} else if len(output.runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

func bitmapsEqual(b, c *Bitmap) error {
	if b.OpWriter != c.OpWriter {
		return errors.New("opWriters not equal")
	}
	if b.opN != c.opN {
		return errors.New("opNs not equal")
	}

	biter, _ := b.Containers.Iterator(0)
	citer, _ := c.Containers.Iterator(0)
	bn, cn := biter.Next(), citer.Next()
	for ; bn && cn; bn, cn = biter.Next(), citer.Next() {
		bk, bc := biter.Value()
		ck, cc := citer.Value()
		if bk != ck {
			return errors.New("keys not equal")
		}
		if !bc.equals(cc) {
			return errors.New("containers not equal")
		}
	}
	if bn && !cn || cn && !bn {
		return errors.New("different numbers of containers")
	}

	return nil
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

func popcountSlice(s []uint64) uint64 {
	cnt := uint64(0)
	for _, x := range s {
		cnt += popcount(x)
	}
	return cnt
}

func popcountMaskSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] &^ m[i])
	}
	return cnt
}

func popcountAndSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] & m[i])
	}
	return cnt
}

func popcountOrSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] | m[i])
	}
	return cnt
}

func popcountXorSlice(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcount(s[i] ^ m[i])
	}
	return cnt
}