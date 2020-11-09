package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
)

// type OFFTYPE int64

type Tree struct {
	file      *os.File
	blockSize uint32
	fileSize  int64
}

// Node defines the node structure
// on disk:
// [datalen][...]
type Node struct {
	IsActive bool // determine if this node on disk is valid for the tree
	IsLeaf   bool
	Children []int64 // record children's offset
	Self     int64
	Next     int64
	Prev     int64
	Parent   int64
	Keys     []string
	Records  []string
}

func NewTree(filename string) (*Tree, error) {
	t := &Tree{}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	t.file = file

	var stat syscall.Statfs_t
	if err = syscall.Statfs(filename, &stat); err != nil {
		return nil, err
	}

	t.blockSize = stat.Bsize

	fstat, err := t.file.Stat()
	if err != nil {
		return nil, err
	}

	t.fileSize = fstat.Size()

	// already has file content
	if t.fileSize != 0 {

	}

	return t, nil
}

func (t *Tree) reconstructRootNode() error {

	return nil
}

func (t *Tree) seekNode(node *Node, off int64) error {
	if node == nil {
		return fmt.Errorf("cannot search nil node")
	}

	buf := make([]byte, 8)
	if n, err := t.file.ReadAt(buf, off); err != nil {
		return err
	} else if n != 8 {
		return fmt.Errorf("read at %v from %v, expect len = %v but got %v", off, t.file.Name(), 8, n)
	}

	bs := bytes.NewBuffer(buf)
	var dataLen int
	if err := binary.Read(bs, binary.LittleEndian, &dataLen); err != nil {
		return err
	}

	if dataLen+8 > int(t.blockSize) {
		return fmt.Errorf("node length invalid: %v, the block size is %v", dataLen, t.blockSize)
	}

	buf = make([]byte, dataLen)
	if n, err := t.file.ReadAt(buf, off+8); err != nil {
		return err
	} else if n != dataLen {
		return fmt.Errorf("read at %v from %v, expect len = %v but got %v", off, t.file.Name(), 8, n)
	}

	bs = bytes.NewBuffer(buf)

	return nil
}

func main() {
	fmt.Println(NewTree("test.db"))
}
