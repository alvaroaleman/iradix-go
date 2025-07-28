package iradix

import (
	"bytes"
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
	currentNode := i.root

	for len(key) > 0 {
		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return len(n.path) > 0 && n.path[0] == key[0]
		})

		if childIdx == -1 {
			return *new(T), false
		}

		child := currentNode.children[childIdx]
		if !bytes.HasPrefix(key, child.path) {
			return *new(T), false
		}

		key = key[len(child.path):]
		currentNode = child
	}

	if currentNode.val != nil {
		return *currentNode.val, true
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
	for len(key) > 0 {
		childIdx := slices.IndexFunc(currentNode.children, func(n *node[T]) bool {
			return len(n.path) > 0 && n.path[0] == key[0]
		})

		if childIdx == -1 {
			newChild := &node[T]{
				path: slices.Clone(key),
				val:  &val,
			}
			currentNode.children = append(currentNode.children, newChild)
			return oldVal, existed, &Iradix[T]{root: newRoot}
		}

		child := currentNode.children[childIdx]
		commonLen := commonPrefixLen(key, child.path)

		if commonLen == len(child.path) {
			newChild := copyNode(child)
			currentNode.children[childIdx] = newChild
			currentNode = newChild
			key = key[commonLen:]
		} else {
			splitNode := &node[T]{
				path:     slices.Clone(child.path[:commonLen]),
				children: []*node[T]{copyNode(child)},
			}

			splitNode.children[0].path = slices.Clone(child.path[commonLen:])

			if commonLen == len(key) {
				splitNode.val = &val
			} else {
				newChild := &node[T]{
					path: slices.Clone(key[commonLen:]),
					val:  &val,
				}
				splitNode.children = append(splitNode.children, newChild)
			}

			currentNode.children[childIdx] = splitNode
			return oldVal, existed, &Iradix[T]{root: newRoot}
		}
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
			return len(n.path) > 0 && n.path[0] == key[0]
		})

		child := currentNode.children[childIdx]
		parents = append(parents, currentNode)
		childIndices = append(childIndices, childIdx)
		currentNode = copyNode(child)
		parents[len(parents)-1].children[childIdx] = currentNode
		key = key[len(currentNode.path):]
	}

	if currentNode.val != nil {
		oldVal, existed = *currentNode.val, true
		currentNode.val = nil
	}

	// Clean up empty nodes and compress single-child chains
	for idx := len(parents) - 1; idx >= 0; idx-- {
		parent := parents[idx]
		childIdx := childIndices[idx]

		if currentNode.val == nil && len(currentNode.children) == 0 {
			parent.children = slices.Delete(parent.children, childIdx, childIdx+1)
		} else if currentNode.val == nil && len(currentNode.children) == 1 {
			onlyChild := currentNode.children[0]
			currentNode.path = append(slices.Clone(currentNode.path), onlyChild.path...)
			currentNode.val = onlyChild.val
			currentNode.children = onlyChild.children
		} else {
			break
		}

		currentNode = parent
	}

	return oldVal, existed, &Iradix[T]{root: newRoot}
}

func (i Iradix[T]) Iterate() iter.Seq2[[]byte, T] {
	return func(yield func([]byte, T) bool) {
		var iterate func(prefix []byte, n *node[T])
		iterate = func(prefix []byte, n *node[T]) {
			currentPrefix := prefix
			if n != i.root {
				currentPrefix = append(slices.Clone(prefix), n.path...)
			}
			if n.val != nil {
				if !yield(currentPrefix, *n.val) {
					iterate = func(prefix []byte, n *node[T]) {}
					return
				}
			}
			for _, child := range n.children {
				iterate(currentPrefix, child)
			}
		}
		iterate(nil, i.root)
	}
}

type node[T any] struct {
	path     []byte
	val      *T
	children []*node[T]
}

func copyNode[T any](n *node[T]) *node[T] {
	return &node[T]{
		path:     slices.Clone(n.path),
		val:      n.val,
		children: slices.Clone(n.children),
	}
}

func commonPrefixLen(a, b []byte) int {
	maxLen := min(len(a), len(b))
	for i := 0; i < maxLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return maxLen
}
