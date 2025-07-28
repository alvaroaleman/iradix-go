package iradix

import (
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

type testItem struct {
	key    []byte
	val    string
	oldVal string
}

func TestIradixInsertGetDelete(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		iradix *Iradix[string]
		items  []testItem
	}{
		{
			name:   "Single item create",
			iradix: New[string](),
			items: []testItem{{
				key: []byte("foo"),
				val: "foo-val",
			}},
		},
		{
			name:   "Empty key single item create",
			iradix: New[string](),
			items: []testItem{{
				key: nil,
				val: "foo-val",
			}},
		},
		{
			name:   "Multiple items with shared prefix",
			iradix: New[string](),
			items: []testItem{
				{
					key: []byte("foo"),
					val: "foo-val",
				},
				{
					key: []byte("fom"),
					val: "fom-val",
				},
			},
		},
		{
			name:   "Multiple items with distinct prefix",
			iradix: New[string](),
			items: []testItem{
				{
					key: []byte("foo"),
					val: "foo-val",
				},
				{
					key: []byte("bar"),
					val: "bar-val",
				},
			},
		},
		{
			name:   "All nodes used",
			iradix: New[string](),
			items: []testItem{
				{
					key: nil,
					val: "empty-val",
				},
				{
					key: []byte("f"),
					val: "f-val",
				},
				{
					key: []byte("fo"),
					val: "fo-val",
				},
				{
					key: []byte("foo"),
					val: "foo-val",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tree := tc.iradix
			// Delete of items in empty tree
			tree = validateDelete(t, tree, false, tc.items...)

			// Item by item create->get->delete
			for _, item := range tc.items {
				tree = validateInsert(t, tree, item)

				itemsExcludingCurrent := slices.DeleteFunc(slices.Clone(tc.items), func(i testItem) bool {
					return reflect.DeepEqual(item, i)
				})
				tree = validateDelete(t, tree, false, itemsExcludingCurrent...)

				val, exists := tree.Get(item.key)
				require.True(t, exists)
				require.Equal(t, item.val, val)

				tree = validateDelete(t, tree, true, item)
			}

			// Batch create, get, list and delete
			tree = validateInsert(t, tree, tc.items...)

			for _, item := range tc.items {
				val, exists := tree.Get(item.key)
				require.True(t, exists)
				require.Equal(t, item.val, val)
			}

			idx := 0
			for k, v := range tree.Iterate() {
				if idx == len(tc.items) {
					t.Fatalf("got superfluous item: key=%v, val=%v", k, v)
				}
				require.Equal(t, tc.items[idx].key, k)
				require.Equal(t, tc.items[idx].val, v)
				idx++
			}

			tree = validateDelete(t, tree, true, tc.items...)
		})
	}
}

func validateTree[T any](t *testing.T, tree *Iradix[T]) {
	t.Helper()
	if tree.root == nil {
		return
	}
	var iterate func(n *node[T], parents []*node[T])
	iterate = func(n *node[T], parents []*node[T]) {
		t.Helper()
		if n.val == nil && len(n.children) == 0 && n != tree.root {
			t.Errorf("found empty node, parents: %+v", parents)
		}
		seenChildKeys := map[byte]struct{}{}
		for _, child := range n.children {
			iterate(child, append(parents, n))
			_, seen := seenChildKeys[child.key]
			if seen {
				t.Errorf("found two children with key %v", child.key)
			}
			seenChildKeys[child.key] = struct{}{}
		}
	}

	iterate(tree.root, nil)
}

func validateInsert(t *testing.T, tree *Iradix[string], items ...testItem) *Iradix[string] {
	t.Helper()
	oldVal, existed := "", false
	for idx, item := range items {
		originalTree := tree
		originalTreeDump := spew.Sdump(tree)
		oldVal, existed, tree = tree.Insert(item.key, item.val)
		newTree := spew.Sdump(tree)
		validateTree(t, tree)
		require.Equal(t,
			item.oldVal != "",
			existed,
			"insert: presence of item %v incorrect\ntree: %s\noriginal tree: %s", item.key, newTree, originalTreeDump,
		)
		require.Equal(t, item.oldVal != "", existed)
		require.Equal(t, item.oldVal, oldVal)
		require.Equal(t, originalTreeDump, spew.Sdump(originalTree), "original tree should be unmodified")

		validateDelete(t, tree, false, items[idx+1:]...)
	}

	return tree
}

func validateDelete(t *testing.T, tree *Iradix[string], expectPresent bool, items ...testItem) *Iradix[string] {
	t.Helper()
	oldVal, existed := "", false
	for _, item := range items {
		originalTree := spew.Sdump(tree)
		oldVal, existed, tree = tree.Delete(item.key)
		validateTree(t, tree)
		newTree := spew.Sdump(tree)
		require.Equal(t,
			expectPresent,
			existed,
			"delete: presence of item %v incorrect\ntree: %s\noriginal tree: %s", item.key, newTree, originalTree,
		)
		expectedOldVal := ""
		if expectPresent {
			expectedOldVal = item.val
		} else if originalTree != newTree {
			t.Errorf("Tree was manipulated during deletion of item that didn't exist: %s", cmp.Diff(originalTree, newTree))
		}
		require.Equal(t, expectedOldVal, oldVal)

		_, exists := tree.Get(item.key)
		require.False(t, exists, "deleted item %s still exists", item.key)
	}

	return tree
}

func TestParallelInsertGet(t *testing.T) {
	t.Parallel()
	tree := New[string]()

	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
		tree.Insert([]byte("foo"), "something")
		wg.Done()
	}()

	go func() {
		tree.Delete([]byte("foo"))
		wg.Done()
	}()

	go func() {
		tree.Get([]byte("foo"))
		wg.Done()
	}()
}

func TestParallelInsertDelete(t *testing.T) {
	t.Parallel()
	tree := New[string]()
	_, _, tree = tree.Insert([]byte("foo"), "something")

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		tree.Delete([]byte("foo"))
		wg.Done()
	}()

	go func() {
		tree.Get([]byte("foo"))
		wg.Done()
	}()
}
