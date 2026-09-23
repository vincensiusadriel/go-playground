package memcache

import (
	"sync"
	"testing"
	"time"
)

func TestSetGetRemove(t *testing.T) {
	m := New()
	defer m.Close()

	if _, err := m.Get("missing"); err != ErrNotFound {
		t.Fatalf("Get missing: got %v, want ErrNotFound", err)
	}

	m.Set("k", 42, time.Hour)
	v, err := m.Get("k")
	if err != nil || v != 42 {
		t.Fatalf("Get k: got (%v, %v), want (42, nil)", v, err)
	}

	if err := m.Remove("k"); err != nil {
		t.Fatalf("Remove k: %v", err)
	}
	if _, err := m.Get("k"); err != ErrNotFound {
		t.Fatalf("Get after remove: got %v, want ErrNotFound", err)
	}
	if err := m.Remove("k"); err != ErrNotFound {
		t.Fatalf("Remove missing: got %v, want ErrNotFound", err)
	}
}

func TestExpiry(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("k", "v", 50*time.Millisecond)
	if v, err := m.Get("k"); err != nil || v != "v" {
		t.Fatalf("immediate Get: got (%v, %v), want (v, nil)", v, err)
	}

	time.Sleep(100 * time.Millisecond)
	if _, err := m.Get("k"); err != ErrNotFound {
		t.Fatalf("expired Get: got %v, want ErrNotFound", err)
	}
}

func TestZeroAndNegativeTTLNeverExpire(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("zero", 1, 0)
	m.Set("neg", 2, -time.Second)

	time.Sleep(1500 * time.Millisecond)

	if v, err := m.Get("zero"); err != nil || v != 1 {
		t.Fatalf("ttl=0 Get: got (%v, %v), want (1, nil)", v, err)
	}
	if v, err := m.Get("neg"); err != nil || v != 2 {
		t.Fatalf("ttl<0 Get: got (%v, %v), want (2, nil)", v, err)
	}
}

func TestResetInvalidatesOldHeapEntry(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("k", "old", 50*time.Millisecond)
	m.Set("k", "new", time.Hour)

	time.Sleep(150 * time.Millisecond)

	v, err := m.Get("k")
	if err != nil || v != "new" {
		t.Fatalf("Get after re-set: got (%v, %v), want (new, nil)", v, err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := New()
	defer m.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := string(rune('a' + n%4))
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				m.Set(key, n, time.Duration(n%3)*time.Millisecond)
				m.Get(key)
				m.Remove(key)
			}
		}(i)
	}
	wg.Wait()
}

func TestCleanerDoesNotDeleteRefreshedKey(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("k", "v1", 10*time.Millisecond)

	for i := 0; i < 5; i++ {
		m.Set("k", "v2", time.Hour)
		time.Sleep(20 * time.Millisecond)
	}

	if v, err := m.Get("k"); err != nil || v != "v2" {
		t.Fatalf("refreshed key: got (%v, %v), want (v2, nil)", v, err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	m := New()
	m.Close()
	m.Close()
}

func TestReSetDoesNotGrowHeap(t *testing.T) {
	m := New()
	defer m.Close()

	for i := 0; i < 10; i++ {
		m.Set("k", i, 24*time.Hour)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hp.Len() != 1 {
		t.Fatalf("heap len after 10 re-Sets: got %d, want 1", m.hp.Len())
	}
	if len(m.hp.index) != 1 {
		t.Fatalf("index len after 10 re-Sets: got %d, want 1", len(m.hp.index))
	}
}

func TestRemoveEvictsHeapItem(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("k", "v", 24*time.Hour)
	if err := m.Remove("k"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hp.Len() != 0 {
		t.Fatalf("heap len after Remove: got %d, want 0", m.hp.Len())
	}
	if len(m.hp.index) != 0 {
		t.Fatalf("index len after Remove: got %d, want 0", len(m.hp.index))
	}
}

func TestZeroTTLOverwriteEvictsHeapItem(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("k", "v", time.Hour)
	m.Set("k", "forever", 0)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hp.Len() != 0 {
		t.Fatalf("heap len after ttl=0 overwrite: got %d, want 0", m.hp.Len())
	}
	if len(m.hp.index) != 0 {
		t.Fatalf("index len after ttl=0 overwrite: got %d, want 0", len(m.hp.index))
	}
}

func TestHeapIndexConsistency(t *testing.T) {
	m := New()
	defer m.Close()

	m.Set("a", 1, time.Hour)
	m.Set("b", 2, time.Minute)
	m.Set("c", 3, time.Second)
	m.Set("a", 10, 30*time.Minute) // re-Set: evicts + repushes
	m.Remove("b")
	m.Set("d", 4, time.Second)
	m.Set("c", 30, 2*time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hp.Len() != len(m.hp.index) {
		t.Fatalf("len mismatch: heap=%d index=%d", m.hp.Len(), len(m.hp.index))
	}
	for i, it := range m.hp.items {
		pos, ok := m.hp.index[it.key]
		if !ok {
			t.Fatalf("items[%d] key %q missing from index", i, it.key)
		}
		if pos != i {
			t.Fatalf("items[%d] key %q: index says %d", i, it.key, pos)
		}
	}
	// live keys with ttl>0: a, c, d → heap size 3
	if m.hp.Len() != 3 {
		t.Fatalf("heap len: got %d, want 3", m.hp.Len())
	}
}

func TestHeapSizeBoundedByLiveKeys(t *testing.T) {
	m := New()
	defer m.Close()

	for i := 0; i < 100; i++ {
		m.Set("k", i, 24*time.Hour)
		m.Remove("k")
		m.Set("k2", i, 24*time.Hour)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.hp.Len() != 1 {
		t.Fatalf("heap len: got %d, want 1 (only k2)", m.hp.Len())
	}
	if _, ok := m.hp.index["k"]; ok {
		t.Fatal("removed key k still indexed in heap")
	}
	if _, ok := m.hp.index["k2"]; !ok {
		t.Fatal("key k2 missing from index")
	}
}
