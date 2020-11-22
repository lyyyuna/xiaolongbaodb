package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
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
	file       *os.File
	blockSize  uint32
	fileSize   int64
	rootOff    int64
	freeBlocks []int64
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

		if err = t.allocNewFreeNodeInDisk(); err != nil {
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

func (t *Tree) allocNewFreeNodeInDisk() error {

	for off := int64(0); off < t.fileSize; off += BLOCK_SIZE {
		node, err := t.seekNode(off)
		if err != nil {
			return err
		}
		// is inactive == freeblock
		if !node.IsActive {
			t.freeBlocks = append(t.freeBlocks, off)
		}
	}

	next_file := ((t.fileSize + 4095) / 4096) * 4096
	for len(t.freeBlocks) < MAX_FREEBLOCKS {
		t.freeBlocks = append(t.freeBlocks, next_file)
		next_file += BLOCK_SIZE
	}
	t.fileSize = next_file

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

func (t *Tree) Insert(key int, val string) error {
	// if tree is empty, insert it as root
	if t.rootOff == INVALID_OFFSET {
		node, err := t.newNodeFromDisk()
		if err != nil {
			return err
		}
		t.rootOff = node.Self
		node.Keys = append(node.Keys, key)
		node.Values = append(node.Values, val)
		node.IsLeaf = true
		return t.flushNodeToDisk(node)
	}

	// otherwise, insert it as leaf
	return t.insertIntoLeaf(key, val)
}

func (t *Tree) newNodeFromDisk() (*Node, error) {

	if len(t.freeBlocks) == 0 {
		if err := t.allocNewFreeNodeInDisk(); err != nil {
			return nil, err
		}
	}
	newDiskOff := t.freeBlocks[0]
	t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
	node := &Node{
		IsActive: true,
		Self:     newDiskOff,
		Prev:     INVALID_OFFSET,
		Next:     INVALID_OFFSET,
		Parent:   INVALID_OFFSET,
	}

	return node, nil
}

func (t *Tree) flushNodeToDisk(n *Node) error {

	if n == nil {
		panic("nil node to disk")
	}

	if t.file == nil {
		panic("file not specified, tree not initialized?")
	}

	bs := bytes.NewBuffer(make([]byte, 0))

	// isactive
	if err := binary.Write(bs, binary.LittleEndian, n.IsActive); err != nil {
		return err
	}

	// isleaf
	if err := binary.Write(bs, binary.LittleEndian, n.IsLeaf); err != nil {
		return err
	}

	// self
	if err := binary.Write(bs, binary.LittleEndian, n.Self); err != nil {
		return err
	}

	// next
	if err := binary.Write(bs, binary.LittleEndian, n.Next); err != nil {
		return err
	}

	// prev
	if err := binary.Write(bs, binary.LittleEndian, n.Prev); err != nil {
		return err
	}

	// parent
	if err := binary.Write(bs, binary.LittleEndian, n.Parent); err != nil {
		return err
	}

	// children
	childCnt := len(n.Children)
	if err := binary.Write(bs, binary.LittleEndian, childCnt); err != nil {
		return err
	}

	for _, v := range n.Children {
		if err := binary.Write(bs, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	// keys
	keysCnt := len(n.Keys)
	if err := binary.Write(bs, binary.LittleEndian, keysCnt); err != nil {
		return err
	}

	for _, v := range n.Keys {
		if err := binary.Write(bs, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	// values
	valuesCnt := len(n.Values)
	if err := binary.Write(bs, binary.LittleEndian, valuesCnt); err != nil {
		return err
	}

	for _, v := range n.Values {
		if err := binary.Write(bs, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	return nil
}

func (t *Tree) insertIntoLeaf(key int, val string) error {
	leaf, err := t.findLeafNode(key)
	if err != nil {
		return err
	}

	idx, err := leaf.insertKeyValIntoLeaf(key, val)
	if err != nil {
		return err
	}

	// 这里父节点存储的是每个子节点最后一个 key
	// 所以有可能需要更新父节点
	if err := leaf.mayUpdateParentKeys(t, idx); err != nil {
		return err
	}

	// lead no needs to split
	if len(leaf.Keys) <= order {
		return t.flushNodeToDisk(leaf)
	}

	// split the leaf
	newLeaf, err := t.newNodeFromDisk()
	if err != nil {
		return err
	}

	newLeaf.IsLeaf = true
	if err := t.splitLeafIntoTwoLeaves(leaf, newLeaf); err != nil {
		return err
	}

	return t.insertIntoParent(leaf)
}

func (t *Tree) insertIntoParent(leaf *Node) error {
	parentOff := leaf.Parent
	leftOff := leaf.Self
	rightOff := leaf.Next
	key := leaf.Keys[len(leaf.Keys)-1]

	// root?
	if parentOff == INVALID_OFFSET {
		left, err := t.seekNode(leftOff)
		if err != nil {
			return err
		}

		right, err := t.seekNode(rightOff)
		if err != nil {
			return err
		}

		if err := t.newRootNode(left, right); err != nil {
			return err
		}
	}

	// not root
	parent, err := t.seekNode(parentOff)
	if err != nil {
		return err
	}

	// insert into parent's keys
	idx := getIndex(parent.Keys, key)
	parent.Keys = append(parent.Keys, 0)

	for i := len(parent.Keys) - 1; i > idx; i-- {
		parent.Keys[i] = parent.Keys[i-1]
	}
	parent.Keys[idx] = key

	if idx == len(parent.Children) {
		parent.Children = append(parent.Children, rightOff)
		return nil
	}

	tmpChild := parent.Children[idx+1:]
	parent.Children = append(append(parent.Children[:idx+1], rightOff), tmpChild...)

	// if parent no need to split
	if len(parent.Keys) <= order {
		return t.flushNodeToDisk(parent)
	}

	// if parent need to split recursively
	// new parent only half
	newNode, err := t.newNodeFromDisk()
	if err != nil {
		return err
	}

	split := cut(order)

	for i := split; i <= order; i++ {
		newNode.Children = append(newNode.Children, parent.Children[i])
		newNode.Keys = append(newNode.Keys, parent.Keys[i])

		// update original's children's parent
		child, err := t.seekNode(parent.Children[i])
		if err != nil {
			return err
		}

		child.Parent = newNode.Self

		if err := t.flushNodeToDisk(child); err != nil {
			return err
		}
	}

	newNode.Parent = parent.Parent

	// original parent keeps another half
	parent.Children = parent.Children[:split]
	parent.Keys = parent.Keys[:split]

	newNode.Next = parent.Next
	parent.Next = newNode.Self
	newNode.Prev = parent.Self

	// update original parent's next
	if newNode.Next != INVALID_OFFSET {
		oriNextNode, err := t.seekNode(newNode.Next)
		if err != nil {
			return err
		}

		oriNextNode.Prev = newNode.Self

		if err := t.flushNodeToDisk(oriNextNode); err != nil {
			return err
		}
	}

	// flush ori parent & new parent
	if err := t.flushNodeToDisk(parent); err != nil {
		return err
	}

	if err := t.flushNodeToDisk(newNode); err != nil {
		return err
	}

	// update parent&newnode 's parent recursively
	return t.insertIntoParent(parent)
}

func getIndex(keys []int, key int) int {
	idx := sort.Search(len(keys), func(i int) bool {
		return key <= keys[i]
	})

	return idx
}

// newRootNode flush new root to disk
// flush left node
// flush right node
func (t *Tree) newRootNode(left, right *Node) error {
	root, err := t.newNodeFromDisk()
	if err != nil {
		return err
	}

	root.Keys = append(root.Keys, left.Keys[len(left.Keys)-1])
	root.Keys = append(root.Keys, right.Keys[len(right.Keys)-1])
	root.Children = append(root.Children, left.Self)
	root.Children = append(root.Children, right.Self)

	left.Parent = root.Self
	right.Parent = root.Self

	t.rootOff = root.Self

	if err := t.flushNodeToDisk(left); err != nil {
		return err
	}

	if err := t.flushNodeToDisk(right); err != nil {
		return err
	}

	return t.flushNodeToDisk(root)
}

func cut(length int) int {
	return (length + 1) / 2
}

func (t *Tree) splitLeafIntoTwoLeaves(leaf *Node, newLeaf *Node) error {
	split := cut(order)

	// copy half to newleaf
	for i := split; i <= order; i++ {
		newLeaf.Keys = append(newLeaf.Keys, leaf.Keys[i])
		newLeaf.Values = append(newLeaf.Values, leaf.Values[i])
	}

	// leave half in original leaf
	leaf.Keys = leaf.Keys[:split]
	leaf.Values = leaf.Values[:split]

	// adjust relation
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf.Self
	newLeaf.Prev = leaf.Self

	// adjust parent
	newLeaf.Parent = leaf.Parent

	// flush to disk
	if err := t.flushNodeToDisk(leaf); err != nil {
		return err
	}

	if err := t.flushNodeToDisk(newLeaf); err != nil {
		return err
	}

	// adjust original next
	if newLeaf.Next != INVALID_OFFSET {
		nextNode, err := t.seekNode(newLeaf.Next)
		if err != nil {
			return err
		}
		nextNode.Prev = newLeaf.Self

		if err := t.flushNodeToDisk(nextNode); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) findLeafNode(key int) (*Node, error) {
	root, err := t.seekNode(t.rootOff)
	if err != nil {
		return nil, err
	}

	var nodeIterator *Node
	nodeIterator = root
	for !nodeIterator.IsLeaf {
		idx := sort.Search(len(nodeIterator.Keys), func(i int) bool {
			return key <= nodeIterator.Keys[i]
		})

		if idx == len(nodeIterator.Keys) {
			idx = len(nodeIterator.Keys) - 1
		}

		var err error
		nodeIterator, err = t.seekNode(nodeIterator.Children[idx])
		if err != nil {
			return nil, err
		}
	}

	return nodeIterator, nil
}

func (n *Node) insertKeyValIntoLeaf(key int, val string) (int, error) {
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return key <= n.Keys[i]
	})

	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, ErrorHasExistedKey
	}

	n.Keys = append(n.Keys, key)
	n.Values = append(n.Values, val)

	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Values[i] = n.Values[i-1]
	}

	// insert into node's keys
	n.Keys[idx] = key
	n.Values[idx] = val

	return idx, nil
}

// 父节点存储字节点最后一个 key
func (leaf *Node) mayUpdateParentKeys(t *Tree, idx int) error {
	if idx == len(leaf.Keys)-1 && leaf.Parent != INVALID_OFFSET {
		key := leaf.Keys[len(leaf.Keys)-1]
		updateNodeOff := leaf.Parent

		var nodeParentIterator, nodeCurIterator *Node
		var err error
		nodeParentIterator = nil
		nodeCurIterator = leaf
		for updateNodeOff != INVALID_OFFSET && idx == len(nodeCurIterator.Keys)-1 {
			nodeParentIterator, err = t.seekNode(updateNodeOff)
			if err != nil {
				return err
			}

			for k, v := range nodeParentIterator.Children {
				if v == nodeCurIterator.Self {
					idx = k
					break
				}
			}

			nodeParentIterator.Keys[idx] = key

			t.flushNodeToDisk(nodeParentIterator)

			updateNodeOff = nodeParentIterator.Parent
			nodeCurIterator = nodeParentIterator
		}
	}

	return nil
}

func main() {
	fmt.Println(NewTree("test.db"))
}
