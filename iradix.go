package iradix

import (
	"iter"
	"reflect"
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

func (i *Iradix[T]) Insert(key []byte, val T) (oldVal T, existed bool, newTree *Iradix[T]) {
	if oldVal, exists := i.Get(key); exists && reflect.DeepEqual(oldVal, val) {
		return oldVal, true, i
	}
	newRoot := copyNode(i.root)
	if len(key) == 0 {
		if newRoot.val != nil {
			oldVal, existed = *newRoot.val, true
		}
		newRoot.val = &val
		return oldVal, existed, &Iradix[T]{root: newRoot}
	}

	currentNode := newRoot
	for ; len(key) > 0; key = key[1:] {
		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return n.key == key[0]
		})
		if childIdx == -1 {
			newChild := &node[T]{
				key: key[0],
			}
			currentNode.children = append(currentNode.children, newChild)
			currentNode = newChild
			continue
		}

		newChild := copyNode(currentNode.children[childIdx])
		currentNode.children[childIdx] = newChild
		currentNode = newChild
	}

	if currentNode.val != nil {
		oldVal, existed = *currentNode.val, true
	}
	currentNode.val = &val

	return oldVal, existed, &Iradix[T]{root: newRoot}
}

func (i *Iradix[T]) Delete(key []byte) (oldVal T, existed bool, newTree *Iradix[T]) {
	if _, exists := i.Get(key); !exists {
		return oldVal, existed, i
	}

	newRoot := copyNode(i.root)
	var parents []*node[T]
	var childIndices []int

	currentNode := newRoot
	for len(key) > 0 {
		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return n.key == key[0]
		})

		parents = append(parents, currentNode)
		childIndices = append(childIndices, childIdx)
		currentNode = copyNode(currentNode.children[childIdx])
		parents[len(parents)-1].children[childIdx] = currentNode
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

	return oldVal, existed, &Iradix[T]{root: newRoot}
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

func copyNode[T any](n *node[T]) *node[T] {
	return &node[T]{
		key:      n.key,
		val:      n.val,
		children: slices.Clone(n.children),
	}
}
