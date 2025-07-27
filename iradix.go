package iradix

import (
	"iter"
	"slices"
)

func New[T any]() *Iradix[T] {
	return &Iradix[T]{root: &node[T]{}}
}

type Iradix[T any] struct {
	root *node[T]
}

func (i *Iradix[T]) Get(key []byte) (T, bool) {
	for currentNode := i.root; currentNode != nil; {
		if len(key) == 0 {
			if currentNode.val != nil {
				return *currentNode.val, true
			}
			break
		}

		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return n.key == key[0]
		})
		if childIdx == -1 {
			break
		}

		key = key[1:]
		currentNode = currentNode.children[childIdx]
	}

	return *new(T), false
}

func (i *Iradix[T]) Insert(key []byte, val T) (oldVal T, existed bool) {
	for currentNode := i.root; ; {
		if len(key) == 0 {
			if currentNode.val != nil {
				oldVal, existed = *currentNode.val, true
			}
			currentNode.val = &val
			return oldVal, existed
		}

		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return n.key == key[0]
		})
		if childIdx == -1 {
			currentNode.children = append(currentNode.children, &node[T]{key: key[0]})
			childIdx = len(currentNode.children) - 1
		}

		key = key[1:]
		currentNode = currentNode.children[childIdx]
	}
}

func (i *Iradix[T]) Delete(key []byte) (oldVal T, existed bool) {
	var parents []*node[T]
	var childIndices []int

	currentNode := i.root
	for len(key) > 0 {
		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return n.key == key[0]
		})
		if childIdx == -1 {
			return oldVal, existed
		}

		parents = append(parents, currentNode)
		childIndices = append(childIndices, childIdx)
		currentNode = currentNode.children[childIdx]
		key = key[1:]
	}

	if currentNode.val != nil {
		oldVal, existed = *currentNode.val, true
		currentNode.val = nil
	}

	// Clean up empty nodes
	for idx := len(parents) - 1; idx >= 0 && currentNode.val == nil && len(currentNode.children) == 0; idx-- {
		parent := parents[idx]
		childIdx := childIndices[idx]
		parent.children = slices.Delete(parent.children, childIdx, childIdx+1)
		currentNode = parent
	}

	return oldVal, existed
}

func (i Iradix[T]) Iterate() iter.Seq2[[]byte, T] {
	return func(yield func([]byte, T) bool) {
		var iterate func(prefix []byte, n *node[T])
		iterate = func(prefix []byte, n *node[T]) {
			if n != i.root {
				prefix = append(prefix, n.key)
			}
			if n.val != nil {
				if !yield(prefix, *n.val) {
					iterate = func(prefix []byte, n *node[T]) {}
					return
				}
			}
			for _, child := range n.children {
				iterate(prefix, child)
			}
		}
		iterate(nil, i.root)
	}
}

type node[T any] struct {
	key      byte
	val      *T
	children []*node[T]
}
