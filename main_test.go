package main

import (
	"testing"
)

func TestInsert(t *testing.T) {
	tree, err := NewTree("test.db")
	if err != nil {
		t.Fatal(err)
	}

	if err := tree.Insert(1, "test1"); err != nil {
		t.Fatal(err)
	}

	if err := tree.PrintTree(); err != nil {
		t.Fatal(err)
	}
}
