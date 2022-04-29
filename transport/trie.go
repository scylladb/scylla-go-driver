package transport

import (
	"sync"

	"github.com/mmatczuk/scylla-go-driver/frame"
)

type trie struct {
	next map[frame.UUID]*trie
	path []*Node
	mu   sync.RWMutex
}

func trieRoot() trie {
	return trie{
		next: make(map[frame.UUID]*trie),
	}
}

func newTrie(node *Node, parent *trie) *trie {
	return &trie{
		next: make(map[frame.UUID]*trie),
		path: append(parent.path, node),
	}
}

func (t *trie) Next(node *Node) *trie {
	t.mu.RLock()
	n, ok := t.next[node.hostID]
	t.mu.RUnlock()
	if ok {
		return n
	}

	t.mu.Lock()
	n = newTrie(node, t)
	t.next[node.hostID] = n
	t.mu.Unlock()
	return n
}

func (t *trie) Path() []*Node {
	return t.path
}
