package reverse_dns

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimpleReverseDNSLookup(t *testing.T) {
	d := newReverseDNSCache(60*time.Second, 1*time.Second, -1)
	defer d.stop()

	d.resolver = &localResolver{}
	answer, err := d.lookup("127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, []string{"localhost"}, answer)
	err = blockAllWorkers(t.Context(), d)
	require.NoError(t, err)

	// do another request with no workers available.
	// it should read from cache instantly.
	answer, err = d.lookup("127.0.0.1")
	require.NoError(t, err)
	require.Equal(t, []string{"localhost"}, answer)

	require.Len(t, d.cache, 1)
	require.Len(t, d.expireList, 1)
	d.cleanup()
	require.Len(t, d.expireList, 1) // ttl hasn't hit yet.

	stats := d.getStats()

	require.EqualValues(t, 0, stats.cacheExpire)
	require.EqualValues(t, 1, stats.cacheMiss)
	require.EqualValues(t, 1, stats.cacheHit)
	require.EqualValues(t, 1, stats.requestsFilled)
	require.EqualValues(t, 0, stats.requestsAbandoned)
}

func TestParallelReverseDNSLookup(t *testing.T) {
	d := newReverseDNSCache(1*time.Second, 1*time.Second, -1)
	defer d.stop()

	d.resolver = &localResolver{}
	var answer1, answer2 []string
	var err1, err2 error
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		answer1, err1 = d.lookup("127.0.0.1")
		wg.Done()
	}()
	go func() {
		answer2, err2 = d.lookup("127.0.0.1")
		wg.Done()
	}()

	wg.Wait()

	require.NoError(t, err1)
	require.NoError(t, err2)

	t.Log(answer1)
	t.Log(answer2)

	require.Equal(t, []string{"localhost"}, answer1)
	require.Equal(t, []string{"localhost"}, answer2)

	require.Len(t, d.cache, 1)

	stats := d.getStats()

	require.EqualValues(t, 1, stats.cacheMiss)
	require.EqualValues(t, 1, stats.cacheHit)
}

func TestUnavailableDNSServerRespectsTimeout(t *testing.T) {
	d := newReverseDNSCache(0, 1, -1)
	defer d.stop()

	d.resolver = &timeoutResolver{}

	result, err := d.lookup("192.153.33.3")
	require.Error(t, err)
	require.Equal(t, errTimeout, err)

	require.Nil(t, result)
}

func TestCleanupHappens(t *testing.T) {
	ttl := 100 * time.Millisecond
	d := newReverseDNSCache(ttl, 1*time.Second, -1)
	defer d.stop()

	d.resolver = &localResolver{}
	_, err := d.lookup("127.0.0.1")
	require.NoError(t, err)

	require.Len(t, d.cache, 1)

	time.Sleep(ttl) // wait for cache entry to expire.
	d.cleanup()
	require.Empty(t, d.expireList)

	stats := d.getStats()

	require.EqualValues(t, 1, stats.cacheExpire)
	require.EqualValues(t, 1, stats.cacheMiss)
	require.EqualValues(t, 0, stats.cacheHit)
}

func TestLookupTimeout(t *testing.T) {
	d := newReverseDNSCache(10*time.Second, 10*time.Second, -1)
	defer d.stop()

	d.resolver = &timeoutResolver{}
	_, err := d.lookup("127.0.0.1")
	require.Error(t, err)
	require.EqualValues(t, 1, d.getStats().requestsAbandoned)
}

type timeoutResolver struct{}

func (*timeoutResolver) LookupAddr(context.Context, string) (names []string, err error) {
	return nil, errors.New("timeout")
}

type localResolver struct{}

func (*localResolver) LookupAddr(context.Context, string) (names []string, err error) {
	return []string{"localhost"}, nil
}

func TestAbandonedLookupCachesNegativeResult(t *testing.T) {
	// Use a very short TTL so we can test expiration
	ttl := 5 * time.Second
	lookupTimeout := 50 * time.Millisecond
	d := newReverseDNSCache(ttl, lookupTimeout, 2)
	defer d.stop()

	d.resolver = &timeoutResolver{}

	// First lookup: should timeout and cache a negative result
	result, err := d.lookup("10.0.0.1")
	require.Error(t, err)
	require.Nil(t, result)

	stats := d.getStats()
	require.EqualValues(t, 1, stats.cacheMiss, "first lookup should be a cache miss")
	require.EqualValues(t, 1, stats.requestsAbandoned, "first lookup should be abandoned")

	// Second lookup for the same IP: should hit the negative cache entry
	// instead of spawning a new lookup goroutine
	result, err = d.lookup("10.0.0.1")
	require.NoError(t, err, "negative cache hit should not return error")
	require.Nil(t, result, "negative cache should return nil domains")

	stats = d.getStats()
	require.EqualValues(t, 1, stats.cacheMiss, "second lookup should NOT be a cache miss")
	require.EqualValues(t, 1, stats.cacheHit, "second lookup should be a cache hit")
	require.EqualValues(t, 1, stats.requestsAbandoned, "no additional lookups should be abandoned")
}

func TestAbandonedLookupDoesNotRetryForever(t *testing.T) {
	// This test verifies that under load, the same unreachable IP does not
	// create an infinite retry loop. Without negative caching, each timeout
	// deletes the cache entry, and the next metric triggers a new lookup.
	ttl := 5 * time.Second
	lookupTimeout := 50 * time.Millisecond
	workerCount := 2
	d := newReverseDNSCache(ttl, lookupTimeout, workerCount)
	defer d.stop()

	d.resolver = &timeoutResolver{}

	// Simulate 100 lookups for the same unreachable IP
	for i := 0; i < 100; i++ {
		d.lookup("192.168.1.1") //nolint:errcheck // we expect errors
	}

	stats := d.getStats()
	// With negative caching: 1 miss + 99 hits
	// Without negative caching (old behavior): 100 misses + 0 hits
	require.EqualValues(t, 1, stats.cacheMiss,
		"only the first lookup should be a cache miss; subsequent lookups should hit negative cache")
	require.EqualValues(t, 99, stats.cacheHit,
		"subsequent lookups should hit the negative cache entry")
}

// blockAllWorkers is a test function that eats up all the worker pool space to
// make sure workers are done running and there's no room to acquire a new worker.
func blockAllWorkers(testContext context.Context, d *reverseDNSCache) error {
	return d.sem.Acquire(testContext, int64(d.maxWorkers))
}
