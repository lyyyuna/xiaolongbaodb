package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

var order = 4

const (
	INVALID_OFFSET = 0xdeadbeef
	MAX_FREEBLOCKS = 100
	BLOCK_SIZE     = 4096 // it should call syscall to find the filesystem block size, but i dont know which syscall on windows
)

var ErrorHasExistedKey = errors.New("hasExistedKey")
var ErrorNotFoundKey = errors.New("notFoundKey")
var ErrorInvalidDBFormat = errors.New("invalid db format")

type Tree struct {
	file      *os.File
	blockSize uint32
	fileSize  int64
	rootOff   int64
}

// Node defines the node structure
// on disk:
// [datalen][...]
type Node struct {
	IsActive bool // determine if this node on disk is valid for the tree
	IsLeaf   bool
	Self     int64
	Next     int64
	Prev     int64
	Parent   int64
	Children []int64 // record children's offset
	Keys     []int
	Values   []string
}

func NewTree(filename string) (*Tree, error) {
	t := &Tree{}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	t.file = file

	// var stat syscall.Statfs_t
	// if err = syscall.Statfs(filename, &stat); err != nil {
	// 	return nil, err
	// }

	t.blockSize = BLOCK_SIZE

	fstat, err := t.file.Stat()
	if err != nil {
		return nil, err
	}

	t.fileSize = fstat.Size()

	// already has file content
	if t.fileSize != 0 {
		if err = t.reconstructRootNode(); err != nil {
			return nil, err
		}
	}

	return t, nil
}

func (t *Tree) reconstructRootNode() error {

	var node *Node
	var err error
	// find first valid node
	for off := int64(0); off < t.fileSize; off += int64(t.blockSize) {
		if node, err = t.seekNode(off); err != nil {
			return err
		}
		if node.IsActive {
			break
		}
	}
	if !node.IsActive {
		return ErrorInvalidDBFormat
	}
	// the root node's parent is invalid
	for node.Parent != INVALID_OFFSET {
		if node, err = t.seekNode(node.Parent); err != nil {
			return err
		}
	}

	t.rootOff = node.Self

	return nil
}

func (t *Tree) seekNode(off int64) (*Node, error) {
	node := &Node{
		IsActive: false,
		Self:     INVALID_OFFSET,
		Next:     INVALID_OFFSET,
		Prev:     INVALID_OFFSET,
		Parent:   INVALID_OFFSET,
	}

	buf := make([]byte, 8)
	if n, err := t.file.ReadAt(buf, off); err != nil {
		return nil, err
	} else if n != 8 {
		return nil, fmt.Errorf("read at %v from %v, expect len = %v but got %v", off, t.file.Name(), 8, n)
	}

	bs := bytes.NewBuffer(buf)
	var dataLen int
	if err := binary.Read(bs, binary.LittleEndian, &dataLen); err != nil {
		return nil, err
	}

	if dataLen+8 > int(t.blockSize) {
		return nil, fmt.Errorf("node length invalid: %v, the block size is %v", dataLen, t.blockSize)
	}

	buf = make([]byte, dataLen)
	if n, err := t.file.ReadAt(buf, off+8); err != nil {
		return nil, err
	} else if n != dataLen {
		return nil, fmt.Errorf("read at %v from %v, expect len = %v but got %v", off, t.file.Name(), 8, n)
	}

	bs = bytes.NewBuffer(buf)

	// isactive
	if err := binary.Read(bs, binary.LittleEndian, &node.IsActive); err != nil {
		return nil, err
	}

	// isleaf
	if err := binary.Read(bs, binary.LittleEndian, &node.IsLeaf); err != nil {
		return nil, err
	}

	// self
	if err := binary.Read(bs, binary.LittleEndian, &node.Self); err != nil {
		return nil, err
	}

	// next
	if err := binary.Read(bs, binary.LittleEndian, &node.Next); err != nil {
		return nil, err
	}

	// prev
	if err := binary.Read(bs, binary.LittleEndian, &node.Prev); err != nil {
		return nil, err
	}

	// parent
	if err := binary.Read(bs, binary.LittleEndian, &node.Parent); err != nil {
		return nil, err
	}

	// children
	var childCnt int
	if err := binary.Read(bs, binary.LittleEndian, &childCnt); err != nil {
		return nil, err
	}
	node.Children = make([]int64, childCnt)
	for i := 0; i < childCnt; i++ {
		var child int64
		if err := binary.Read(bs, binary.LittleEndian, &child); err != nil {
			return nil, err
		}
		node.Children[i] = child
	}

	// keys
	var keysCnt int
	if err := binary.Read(bs, binary.LittleEndian, &keysCnt); err != nil {
		return nil, err
	}
	node.Keys = make([]int, keysCnt)
	for i := 0; i < keysCnt; i++ {
		var key int
		if err := binary.Read(bs, binary.LittleEndian, &key); err != nil {
			return nil, err
		}
		node.Keys[i] = key
	}

	// values
	var valuesCnt int
	if err := binary.Read(bs, binary.LittleEndian, &valuesCnt); err != nil {
		return nil, err
	}
	node.Values = make([]string, valuesCnt)
	for i := 0; i < valuesCnt; i++ {
		var strLen int
		if err := binary.Read(bs, binary.LittleEndian, &strLen); err != nil {
			return nil, err
		}
		strBytes := make([]byte, strLen)
		if err := binary.Read(bs, binary.LittleEndian, &strBytes); err != nil {
			return nil, err
		}
		node.Values[i] = string(strBytes)
	}

	return node, nil
}

func main() {
	fmt.Println(NewTree("test.db"))
}
