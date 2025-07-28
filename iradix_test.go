package iradix

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
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
					key: []byte("fom"),
					val: "fom-val",
				},
				{
					key: []byte("foo"),
					val: "foo-val",
				},
			},
		},
		{
			name:   "Multiple items with distinct prefix",
			iradix: New[string](),
			items: []testItem{
				{
					key: []byte("bar"),
					val: "bar-val",
				},
				{
					key: []byte("foo"),
					val: "foo-val",
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
		{
			name:   "Path splitting - insert causes node split",
			iradix: New[string](),
			items: []testItem{
				{
					key: []byte("test"),
					val: "test-val",
				},
				{
					key: []byte("testing"),
					val: "testing-val",
				},
			},
		},
		{
			name:   "Path splitting - multiple splits",
			iradix: New[string](),
			items: []testItem{
				{
					key: []byte("abcd"),
					val: "abcd-val",
				},
				{
					key: []byte("abcdefgh"),
					val: "abcdefgh-val",
				},
				{
					key: []byte("abcxyz"),
					val: "abcxyz-val",
				},
			},
		},
		{
			name:   "Complex path compression scenario",
			iradix: New[string](),
			items: []testItem{
				{
					key: []byte("namespace"),
					val: "namespace-val",
				},
				{
					key: []byte("namespace/pod-1"),
					val: "posts-val",
				},
				{
					key: []byte("namespace/pod-2/owner-1"),
					val: "owner-1-val",
				},
				{
					key: []byte("namespace/pod-2/owner-2"),
					val: "owner-2-val",
				},
				{
					key: []byte("namespaces"),
					val: "namespaces-val",
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
				require.Equal(t, 1, tree.Len())

				itemsExcludingCurrent := slices.DeleteFunc(slices.Clone(tc.items), func(i testItem) bool {
					return reflect.DeepEqual(item, i)
				})
				tree = validateDelete(t, tree, false, itemsExcludingCurrent...)

				val, exists := tree.Get(item.key)
				require.True(t, exists)
				require.Equal(t, item.val, val)

				tree = validateDelete(t, tree, true, item)
				require.Equal(t, 0, tree.Len())
			}

			// Batch create, get, list and delete
			tree = validateInsert(t, tree, tc.items...)
			require.Equal(t, len(tc.items), tree.Len())

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
			require.Equal(t, 0, tree.Len())
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
		if n.val == nil && len(n.children) < 2 && n != tree.root {
			t.Errorf("found empty node, parents: %+v", parents)
		}
		seenChildKeys := map[byte]struct{}{}
		for _, child := range n.children {
			iterate(child, append(parents, n))
			if len(child.path) > 0 {
				_, seen := seenChildKeys[child.path[0]]
				if seen {
					t.Errorf("found two children with first byte %v", child.path[0])
				}
				seenChildKeys[child.path[0]] = struct{}{}
			}
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

func TestPathCompressionUpdates(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		setup  []testItem
		update testItem
	}{
		{
			name: "Update value in compressed path",
			setup: []testItem{
				{key: []byte("namespacelication/json"), val: "json-val"},
			},
			update: testItem{key: []byte("namespacelication/json"), val: "json-updated", oldVal: "json-val"},
		},
		{
			name: "Update causes path split",
			setup: []testItem{
				{key: []byte("longprefix"), val: "long-val"},
			},
			update: testItem{key: []byte("long"), val: "short-val"},
		},
		{
			name: "Update existing split node",
			setup: []testItem{
				{key: []byte("prefix"), val: "prefix-val"},
				{key: []byte("prefixlong"), val: "prefixlong-val"},
			},
			update: testItem{key: []byte("prefix"), val: "prefix-updated", oldVal: "prefix-val"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tree := New[string]()

			for idx, item := range tc.setup {
				tree = validateInsert(t, tree, item)
				require.Equal(t, idx+1, tree.Len())
			}

			tree = validateInsert(t, tree, tc.update)
			require.Equal(t, len(tc.setup)+1, tree.Len())

			val, exists := tree.Get(tc.update.key)
			require.True(t, exists)
			require.Equal(t, tc.update.val, val)

			// Verify other values are still there
			for _, item := range tc.setup {
				if !slices.Equal(item.key, tc.update.key) {
					val, exists := tree.Get(item.key)
					require.True(t, exists)
					require.Equal(t, item.val, val)
				}
			}

			validateTree(t, tree)
		})
	}
}

func TestPathCompressionDeletion(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		setup  []testItem
		delete []byte
		expect []testItem
	}{
		{
			name: "Delete from compressed path causes merge",
			setup: []testItem{
				{key: []byte("test"), val: "test-val"},
				{key: []byte("testing"), val: "testing-val"},
			},
			delete: []byte("test"),
			expect: []testItem{
				{key: []byte("testing"), val: "testing-val"},
			},
		},
		{
			name: "Delete leaf node with compression",
			setup: []testItem{
				{key: []byte("namespace/pod-1"), val: "pod-1-val"},
				{key: []byte("namespace/pod-2"), val: "pod-2-val"},
				{key: []byte("namespace/pod-3"), val: "pod-3-val"},
			},
			delete: []byte("namespace/pod-3"),
			expect: []testItem{
				{key: []byte("namespace/pod-1"), val: "pod-1-val"},
				{key: []byte("namespace/pod-2"), val: "pod-2-val"},
			},
		},
		{
			name: "Delete causes chain compression",
			setup: []testItem{
				{key: []byte("a"), val: "a-val"},
				{key: []byte("abc"), val: "abc-val"},
				{key: []byte("abcdef"), val: "abcdef-val"},
			},
			delete: []byte("abc"),
			expect: []testItem{
				{key: []byte("a"), val: "a-val"},
				{key: []byte("abcdef"), val: "abcdef-val"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tree := New[string]()

			for idx, item := range tc.setup {
				tree = validateInsert(t, tree, item)
				require.Equal(t, idx+1, tree.Len())
			}

			_, existed, tree := tree.Delete(tc.delete)
			require.True(t, existed)
			require.Equal(t, len(tc.setup)-1, tree.Len())
			validateTree(t, tree)

			for _, item := range tc.expect {
				val, exists := tree.Get(item.key)
				require.True(t, exists, "key %s should exist", item.key)
				require.Equal(t, item.val, val)
			}

			_, exists := tree.Get(tc.delete)
			require.False(t, exists, "deleted key %s should not exist", tc.delete)
		})
	}
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

func BenchmarkIradixWriteRead(b *testing.B) {
	const value = "the value we store"
	for i := 0; i < b.N; i++ {
		tree := New[string]()

		for cycle := 0; cycle < 10; cycle++ {
			// Insert 100 elements with common prefix
			cycle := 1
			prefix := fmt.Sprintf("prefix%d/", cycle)
			for j := 0; j < 100; j++ {
				key := []byte(prefix + strconv.Itoa(j))
				_, _, tree = tree.Insert(key, value)
			}

			// Read 100 elements 3 times with different prefixes
			readPrefixes := []string{
				"prefix" + strconv.Itoa(max(0, cycle-2)) + "_",
				"prefix" + strconv.Itoa(max(0, cycle-1)) + "_",
				"prefix" + strconv.Itoa(cycle) + "_",
			}

			for _, readPrefix := range readPrefixes {
				for j := 0; j < 100; j++ {
					key := []byte(readPrefix + strconv.Itoa(j))
					tree.Get(key)
				}
			}
		}
	}
}
