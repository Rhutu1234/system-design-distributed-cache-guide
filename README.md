# System Design: Distributed Cache

*A capstone system design walkthrough — designing a distributed caching system end to end — covering the core domain model, partitioning and consistent hashing across nodes, replication for availability, eviction policies and their real trade-offs, cache invalidation as the domain's hardest and most classic problem, handling hot keys and cache stampedes, and the specific latency, staleness, and coordination demands that make a distributed cache deceptively subtle to get right at scale.*

---

## Table of Contents

1. [Introduction](#introduction)
2. [Why a Distributed Cache Is a Different Kind of Hard](#1-why-a-distributed-cache-is-a-different-kind-of-hard)
3. [The Core Domain Model](#2-the-core-domain-model)
4. [Partitioning: Consistent Hashing Across Nodes](#3-partitioning-consistent-hashing-across-nodes)
5. [Replication for Availability and Read Scaling](#4-replication-for-availability-and-read-scaling)
6. [Eviction Policies](#5-eviction-policies)
7. [Cache Invalidation: The Hardest Problem in This Domain](#6-cache-invalidation-the-hardest-problem-in-this-domain)
8. [Cache-Aside, Write-Through, and Write-Behind Patterns](#7-cache-aside-write-through-and-write-behind-patterns)
9. [Hot Keys and the Cache Stampede Problem](#8-hot-keys-and-the-cache-stampede-problem)
10. [Handling Node Failure and Rebalancing](#9-handling-node-failure-and-rebalancing)
11. [Serialization and Memory Management](#10-serialization-and-memory-management)
12. [Data Security and Multi-Tenancy](#11-data-security-and-multi-tenancy)
13. [Consistency, Availability, and the CAP Trade-off for a Cache](#12-consistency-availability-and-the-cap-trade-off-for-a-cache)
14. [Scaling the System](#13-scaling-the-system)
15. [Observability for a Distributed Cache](#14-observability-for-a-distributed-cache)
16. [Common Pitfalls](#15-common-pitfalls)
17. [Quick Reference Table](#quick-reference-table)
18. [Conclusion](#conclusion)

---

## Introduction

A distributed cache takes the general system design vocabulary covered in this series' System Design guide — partitioning, replication, eviction, consistency — and applies it to a component whose entire purpose is trading a small amount of staleness for a large amount of speed, which sounds simple until the mechanics of *keeping that staleness small and bounded, across many nodes, under real write traffic* turn out to be one of the genuinely hard, well-known problems in distributed systems. This guide walks through designing such a system end to end, drawing directly on this series' Redis, Distributed Systems, and Consistent Hashing guides, each of which turns out to be direct, load-bearing infrastructure here rather than incidental background — this guide in some ways completes a set with this series' Rate Limiter guide, since both are built on the same Redis and consistent-hashing foundations but apply them toward different ends.

```plaintext
Client → Cache Client Library (consistent hash routing) → Cache Node (partition owner)
                                                                  ↓ (replica sync)
                                                            Replica Node(s)
Cache MISS → Origin Data Store (source of truth) → populate cache → return to client
```

---

## 1. Why a Distributed Cache Is a Different Kind of Hard

### Invalidation is famously one of the two hard problems in computer science, and this system exists entirely to manage it

Most systems covered in this series treat caching as one tool among several. A distributed cache *is* the system, and its central, unavoidable difficulty is the one Phil Karlton's famous line names directly: knowing when cached data has gone stale and needs to be removed or refreshed is genuinely hard, not because the mechanics of eviction are complex, but because it requires the cache (or something coordinating with it) to know about every write to the underlying data that could invalidate what it's holding — this is why Section 6 gets more sustained attention in this guide than perhaps any single section in this series' other capstone guides.

### The cache sits in the latency-critical path, but unlike a rate limiter, it's expected to do real work fast

```plaintext
Per this series' Rate Limiter guide's Section 1: a rate limiter's operation is
  cheap and its latency budget is tight because the check itself is simple.
A cache's operation is ALSO latency-critical, but it's expected to serve
  potentially large payloads fast — the latency budget is tight for a
  DIFFERENT reason: the cache only earns its place if it's meaningfully
  faster than the origin store it's protecting, for every request it serves.
```

Unlike this series' Rate Limiter guide, where the challenge is mostly about correctness under concurrency within a cheap operation, a cache's challenge is sustaining low, predictable latency while serving genuinely variable payload sizes at genuinely high volume — which is why Section 10's serialization and memory management, and Section 5's replication for read scaling, both receive real design attention here in a way they wouldn't for a system just returning a boolean decision.

### A cache should never become a second source of truth, even by accident

A critical, freeing realization for the design that follows: a distributed cache, in the overwhelming majority of real-world designs, does not need to guarantee that every read reflects the absolute latest write — it needs to guarantee that any staleness it does introduce is *bounded, understood, and acceptable* for the specific data being cached. This mirrors the "eventual consistency is fine when the cost of staleness is bounded and low" principle covered throughout this series' Event-Driven Architecture and CQRS discussions, applied here as the cache's foundational design philosophy rather than an occasional trade-off — a cache that accidentally becomes authoritative (the origin store falls behind, or is bypassed entirely) has stopped being a cache and become an unmanaged, undocumented database.

---

## 2. The Core Domain Model

### Modeled deliberately simply, in the same spirit as this series' Rate Limiter and URL Shortener guides

```csharp
public record CacheKey(string Value);
public record CacheEntry(CacheKey Key, byte[] SerializedValue, DateTimeOffset? ExpiresAt, long Version);

public interface IDistributedCache
{
    Task<CacheEntry?> GetAsync(CacheKey key);
    Task SetAsync(CacheKey key, byte[] value, TimeSpan? ttl);
    Task InvalidateAsync(CacheKey key);
}
```

As with this series' Rate Limiter guide's own domain modeling choice, a cache's core operations don't warrant a heavyweight DDD aggregate — the interesting design decisions live in partitioning (Section 3), replication (Section 4), eviction (Section 5), and invalidation (Section 6), not in the shape of the entry itself, which stays intentionally minimal: a key, a serialized value, an optional expiration, and a version for the optimistic-concurrency use cases Section 7 covers.

### The version field earns its place specifically for write-path correctness

```csharp
public async Task<bool> SetIfUnchangedAsync(CacheKey key, byte[] newValue, long expectedVersion)
{
    var current = await GetAsync(key);
    if (current?.Version != expectedVersion) return false; // per this series' OCC discussion
    await SetAsync(key, newValue, version: expectedVersion + 1);
    return true;
}
```

Per this series' High-Volume Transaction Processing guide's Section 7 discussion of optimistic concurrency control, including a version on cache entries — even though a cache is fundamentally a simple key-value store — supports the compare-and-set semantics Section 7's write-through pattern and Section 8's stampede-prevention locking both depend on, closing a real race condition class that a version-less cache would leave open.

---

## 3. Partitioning: Consistent Hashing Across Nodes

### Why a single cache node can't hold, or serve, everything at scale

```plaintext
A single node has a hard ceiling on both memory capacity and request
  throughput — per this series' System Design guide's horizontal scaling
  discussion, sustaining a large working set and high request volume
  requires partitioning the keyspace across many nodes, each holding and
  serving only a fraction of it.
```

As covered in this series' System Design guide's scaling discussion, vertical scaling (a bigger node) buys some headroom but not a fundamentally higher ceiling — a distributed cache needs to partition its keyspace across many nodes from the start, each independently serving reads and writes for the keys it owns.

### Consistent hashing as the standard, and the reason a naive modulo scheme fails badly here

```csharp
uint hash = ConsistentHash(key.Value);
CacheNode owner = _hashRing.GetNodeForHash(hash);
```

Per this series' Consistent Hashing guide (echoed from this series' High-Volume Transaction Processing and Rate Limiter guides), a naive `hash(key) % node_count` scheme remaps nearly every key whenever the node count changes — for a cache specifically, that remapping event is unusually costly: every key that moves to a "new" owner is, from that owner's perspective, a guaranteed cache miss, so a naive scheme turns any scaling event into a temporary, system-wide cache-effectiveness collapse. Consistent hashing (with virtual nodes for even load distribution, per this series' Consistent Hashing guide's detail) keeps that remapping to a small, bounded fraction of keys, which is precisely why it's close to a non-negotiable default for this domain rather than one option among several.

### Client-side routing vs. a proxy layer

```plaintext
Client-side (per this series' Redis Cluster discussion): the calling
  application embeds the hash ring and routes directly to the owning node —
  lowest latency (no extra hop), but every client needs to stay in sync
  with ring topology changes.
Proxy-based (per this series' Twemproxy/Envoy-style discussion): a shared
  proxy layer owns routing logic centrally — simpler for clients, at the
  cost of an added network hop and a new, shared point of potential contention.
```

This is a genuine architectural trade-off worth stating explicitly, per this series' System Design guide's general encouragement to name trade-offs rather than presenting one option as objectively correct: client-side routing (as used by Redis Cluster and similar systems) removes a hop from the latency-critical path (Section 1) at the cost of needing every client to track topology, while a proxy centralizes that complexity at the cost of an extra hop — most high-throughput deployments lean toward client-side routing specifically because Section 1's latency stakes make that extra hop expensive at scale.

---

## 4. Replication for Availability and Read Scaling

### Why a single copy of any given key is a real availability risk

```plaintext
Per Section 3's partitioning: if EACH key lives on exactly one node with no
  replica, that node's failure means every key it owned becomes entirely
  unavailable (or, worse, silently treated as a miss, hammering the origin
  store, per Section 9) until the node recovers or the ring rebalances.
```

As covered in this series' Replication guide, a partitioned system with no replication trades a single global point of failure for many small, per-partition points of failure — still a real availability risk, since losing even one node's worth of keys means real user-facing cache misses (and, per Section 9, potential thundering-herd pressure on the origin store) for that entire slice of the keyspace.

### Leader-follower replication per partition, and the read-scaling benefit that comes with it

```plaintext
Each partition's owning node acts as a WRITE leader for its keys, with one
  or more follower replicas receiving asynchronous updates — followers can
  serve READS (accepting some replication lag, per Section 12), spreading
  read load across more nodes than the partition count alone would allow.
```

Per this series' Database Replication guide's leader-follower pattern, applied here to cache partitions rather than a primary database, this gives two benefits at once: a follower can be promoted quickly if a leader fails (Section 9), and read-heavy workloads (which most caching workloads are, by definition) can be spread across followers — accepting the same bounded, deliberate staleness Section 1 already established as this system's foundational trade-off.

### Synchronous vs. asynchronous replication — a latency-vs-durability trade-off specific to cache data

```plaintext
Given Section 1's framing (a cache is a derived, regenerable copy, never
  the source of truth), most distributed caches choose ASYNCHRONOUS
  replication — losing a just-written cache entry on node failure is a
  regenerable inconvenience (the next read simply misses and repopulates
  from origin), not a genuine data-loss event, so paying synchronous
  replication's latency cost for durability the cache doesn't actually need
  is usually the wrong trade.
```

This is a direct, cache-specific application of this series' Database Replication guide's synchronous-vs-asynchronous trade-off — but resolved differently than it typically would be for a primary data store, precisely because Section 1 already established that cache data is inherently regenerable: paying for synchronous replication's durability guarantee doesn't make sense when the thing being protected was never the authoritative copy to begin with.

---

## 5. Eviction Policies

### Why eviction is necessary at all — memory is the cache's scarcest resource

```plaintext
Unlike an origin data store designed to hold its full dataset, a cache
  typically holds only a WORKING SET — a deliberately bounded subset of
  the full data, sized to fit in memory across the cluster — which means
  something must decide what to evict once that bound is reached.
```

As covered in this series' Caching guide, a cache's memory bound is a deliberate design constraint, not an accident, and the eviction policy is what decides which entries get sacrificed to stay within it — the right policy depends entirely on the actual access pattern of the data being cached, echoing this series' URL Shortener guide's Section 6 discussion of choosing eviction policy deliberately rather than defaulting blindly.

### LRU, LFU, and hybrid policies, and when each actually fits

```plaintext
LRU (least-recently-used): evicts what hasn't been touched in the longest
  time — fits access patterns where recency predicts future access well
  (a user's own recent session data, say).
LFU (least-frequently-used): evicts what's touched least often overall —
  fits access patterns with a stable, skewed popularity distribution (a
  small set of consistently popular items, per this series' URL Shortener
  guide's link-popularity discussion), where a recently-touched-but-rare
  item shouldn't displace a consistently hot one.
```

Per this series' Caching guide's eviction policy comparison, this choice deserves the same deliberate attention this series' URL Shortener guide gives it — a workload with a stable long-tail popularity distribution is better served by LFU or a hybrid (like the LRU-K or ARC policies covered in this series' Advanced Caching discussion) than by plain LRU, which can be fooled by a burst of one-off accesses evicting genuinely popular entries.

### TTL as an orthogonal, complementary mechanism to size-based eviction

```csharp
await _cache.SetAsync(key, value, ttl: TimeSpan.FromMinutes(10)); // expires regardless of memory pressure
```

Worth being explicit that TTL-based expiration and size-based eviction (LRU/LFU) are solving different problems and should both be used together, not treated as alternatives — a TTL bounds how stale an entry can get *regardless* of memory pressure (directly supporting Section 1's "bounded, understood staleness" principle), while size-based eviction manages the memory constraint itself; a cache using only one of the two either grows unboundedly stale or has no mechanism to enforce a maximum staleness independent of how full the cache happens to be.

---

## 6. Cache Invalidation: The Hardest Problem in This Domain

### TTL-based expiration: the simplest strategy, and its real limitation

```plaintext
Setting a TTL and letting entries expire naturally sidesteps needing to
  KNOW about every write — but it means every cached entry is stale for
  up to the full TTL duration after an underlying write, which may or may
  not be acceptable depending on how time-sensitive the specific data is.
```

TTL-based expiration is the default strategy for data where Section 1's "bounded, understood staleness" is genuinely acceptable at the TTL's duration — it requires no coordination with the write path at all, which is a real simplicity advantage, but it's a blunt instrument for data where even brief staleness after a specific write is a real problem (a price change, a permission revocation).

### Explicit invalidation on write: precise, but requires the write path to know about the cache

```csharp
public async Task UpdateProductPriceAsync(ProductId id, decimal newPrice)
{
    await _productStore.UpdatePriceAsync(id, newPrice); // origin write
    await _cache.InvalidateAsync(new CacheKey($"product:{id}")); // explicit, synchronous invalidation
}
```

As covered in this series' Cache Invalidation guide, explicitly invalidating (or updating) the relevant cache key as part of the write path closes Section 6's TTL-window gap precisely, at the cost of coupling the write path to cache-key knowledge — every write path that could affect cached data now needs to know which keys to invalidate, which is a real maintenance burden that grows with the number of distinct places data gets written from.

### Event-driven invalidation: decoupling the write path from cache-key knowledge

```plaintext
Per this series' Event-Driven Architecture guide: the origin store publishes
  a change event (via CDC or an explicit domain event) on every write; a
  SEPARATE cache-invalidation consumer subscribes and invalidates the
  relevant keys — the write path itself never needs to know the cache exists.
```

Per this series' Event-Driven Architecture and Change Data Capture guides, having the origin data store's own write path publish change events (rather than every caller of that write path remembering to invalidate the cache directly) decouples cache-key knowledge from the write path entirely — a genuinely more scalable, maintainable pattern once a system has more than a couple of write paths that can affect the same cached data, echoing this series' Notification System guide's fan-out-from-a-single-trigger-event pattern applied here to invalidation instead of notification delivery.

### Write-invalidate vs. write-update, and the trade-off between them

```plaintext
Write-invalidate: remove the stale entry, let the next read repopulate it
  (cache-aside, Section 7) — simpler, and avoids ever writing a value to
  cache that a concurrent process might be computing differently.
Write-update: push the NEW value directly into cache at write time — saves
  the next reader a cache-miss round trip, but risks a race where two
  concurrent writers' updates interleave incorrectly without care.
```

Per this series' Cache Invalidation guide's comparison, write-invalidate is generally the safer default — it never risks writing an incorrect value into cache, only ever removing a possibly-stale one — while write-update is a legitimate optimization for read-heavy, infrequently-written keys where saving that one round trip meaningfully matters, provided the write path uses Section 2's version field to guard against the interleaving race.

---

## 7. Cache-Aside, Write-Through, and Write-Behind Patterns

### Cache-aside: the most common pattern, and the one this series' other guides default to

```csharp
public async Task<Product> GetProductAsync(ProductId id)
{
    var cached = await _cache.GetAsync(new CacheKey($"product:{id}"));
    if (cached is not null) return Deserialize<Product>(cached.SerializedValue);

    var product = await _productStore.GetAsync(id); // cache MISS — read from origin
    await _cache.SetAsync(new CacheKey($"product:{id}"), Serialize(product), ttl: DefaultTtl);
    return product;
}
```

As covered in this series' Caching guide and applied directly in this series' URL Shortener guide's Section 6, cache-aside puts the application in control of both the read-through-on-miss and the invalidation (Section 6) logic — it's the most common pattern precisely because it requires no special support from the cache itself and composes cleanly with any of Section 6's invalidation strategies.

### Write-through: keeping the cache and origin store synchronized on every write

```plaintext
Every write goes through the CACHE, which synchronously writes to the
  origin store before acknowledging — guarantees the cache is never stale
  immediately after a write, at the cost of adding the origin store's write
  latency to every write path, even ones that don't need to read the value
  back soon.
```

Per this series' Caching guide's write-through pattern, this trades write latency for read-path simplicity and freshness guarantees — a reasonable choice specifically for read-heavy, write-light workloads where the added write-path latency is rarely on a hot path anyway.

### Write-behind (write-back): decoupling write latency from origin durability, at real risk

```plaintext
The cache acknowledges the write immediately and asynchronously flushes to
  the origin store afterward — the fastest write path of the three, but a
  cache-node failure between acknowledgment and flush means a genuinely
  LOST write, not just a stale read — a real durability risk this pattern
  accepts deliberately, per this series' Resilience guide's discussion of
  explicit, informed trade-offs.
```

As covered in this series' Caching guide, write-behind should only be chosen where the write's durability genuinely doesn't matter as much as its latency — echoing this series' URL Shortener guide's Section 9 decision to accept bounded, low-stakes data loss for click analytics — and is a meaningfully riskier choice than either cache-aside or write-through for anything where a lost write would be a real problem.

---

## 8. Hot Keys and the Cache Stampede Problem

### Why a single, extremely popular key breaks the "just add more nodes" assumption

```plaintext
Per this series' Rate Limiter guide's Section 1 and High-Volume Transaction
  Processing guide's Section 7: partitioning distributes AVERAGE load evenly,
  but a single, extremely popular key still lands on exactly one node (or
  its replicas, per Section 4) — that node can be overwhelmed even while
  the cluster's aggregate capacity is nowhere near its limit.
```

This is the same hot-key phenomenon covered in this series' High-Volume Transaction Processing guide's Section 7, applied here to reads rather than writes — a viral piece of content, a globally shared configuration value, or a celebrity's profile in a social app can concentrate an outsized fraction of total request volume onto one key, and by extension, one node or its replica set.

### The cache stampede: what happens when a hot key expires

```plaintext
When a hot key's TTL expires (or it's explicitly invalidated, Section 6),
  EVERY concurrent request for that key misses at once and hits the origin
  store simultaneously — a thundering herd that can genuinely overwhelm an
  origin store sized to handle the CACHED read rate, not the full uncached
  rate all at once.
```

As covered in this series' Cache Stampede discussion, this is a distinct failure mode from ordinary hot-key load — it's a *synchronized* spike triggered by expiration or invalidation, and it's dangerous precisely because it can take down an origin store that was never sized to handle its own full read traffic, since the whole point of caching was to shield it from exactly that.

### Mitigation: locking, probabilistic early expiration, and request coalescing

```csharp
public async Task<Product> GetWithStampedeProtectionAsync(ProductId id)
{
    var cached = await _cache.GetAsync(new CacheKey($"product:{id}"));
    if (cached is not null) return Deserialize<Product>(cached.SerializedValue);

    // Only ONE caller per key actually queries the origin; others wait and receive the result
    return await _singleFlightGroup.DoAsync($"product:{id}", async () =>
    {
        var product = await _productStore.GetAsync(id);
        await _cache.SetAsync(new CacheKey($"product:{id}"), Serialize(product), ttl: DefaultTtl);
        return product;
    });
}
```

Per this series' Cache Stampede guide's mitigation strategies, request coalescing (often called "single-flight," collapsing many concurrent misses for the same key into one actual origin call) directly prevents the thundering-herd scenario above; a complementary technique, probabilistic early expiration (recomputing a value slightly before its TTL genuinely expires, with randomized jitter across many callers so they don't all recompute at exactly the same moment), spreads out the refresh load further still, and both are worth combining for any key with genuinely hot, high-concurrency read traffic.

---

## 9. Handling Node Failure and Rebalancing

### Detecting failure without over-reacting to transient network blips

```plaintext
Per this series' Health Checks guide: a node failing a SINGLE health check
  is not the same as a node being genuinely down — a brief network partition
  or a slow GC pause can trigger false-positive failure detection, which
  matters here because reacting to a false positive means unnecessary
  rebalancing (Section 3) and unnecessary cache-miss churn.
```

As covered in this series' Health Checks guide, failure detection needs a deliberate threshold (consecutive failed checks, or a quorum-based view from multiple observers) before triggering the failover and rebalancing machinery below — reacting too eagerly to a single missed heartbeat trades one problem (a brief availability gap) for a worse one (unnecessary, disruptive rebalancing).

### Failover to a replica, and the brief consistency question it raises

```plaintext
Per Section 4's leader-follower replication: on confirmed leader failure,
  a follower is promoted to leader for that partition — because replication
  is asynchronous (Section 4), the newly promoted leader may be missing the
  last few writes the old leader had accepted but not yet replicated, a
  small, BOUNDED window of potential staleness Section 1 already accepts
  as this system's foundational trade-off.
```

Given Section 4's deliberate choice of asynchronous replication (justified there by cache data being inherently regenerable), failover here can proceed quickly without needing the more careful, latency-costly consensus protocols this series' High-Volume Transaction Processing guide discusses for financial ledger data — the worst case is a handful of very recent writes are lost and simply re-fetched from origin on the next miss, which is an acceptable, bounded cost given what a cache actually is.

### Rebalancing: minimizing disruption during scale-out or scale-in

```plaintext
Per Section 3's consistent hashing: adding or removing a node only remaps
  a small, bounded fraction of the total keyspace — but even that fraction
  represents a temporary wave of guaranteed misses for the affected keys,
  worth smoothing with a gradual, rate-limited migration rather than an
  instantaneous, all-at-once cutover.
```

As covered in this series' Consistent Hashing guide, even the bounded remapping consistent hashing guarantees still represents real, if limited, cache-miss churn during a scaling event — gradually migrating affected keys' ownership (rather than an instantaneous cutover) and allowing the old owner to continue briefly serving reads for keys mid-migration smooths this transition, protecting the origin store from an avoidable, if smaller-scale, version of Section 8's stampede problem.

---

## 10. Serialization and Memory Management

### Serialization format as a genuine performance lever, not an afterthought

```plaintext
Per this series' Serialization guide's comparison: JSON is human-readable
  and universally supported but comparatively slow to (de)serialize and
  verbose on the wire; binary formats (Protocol Buffers, MessagePack)
  serialize faster and use less memory and network bandwidth, at the cost
  of losing human readability for debugging.
```

As covered in this series' Serialization guide, the choice of serialization format directly affects both Section 1's latency budget (serialization/deserialization time is real, measurable overhead on every cache operation) and the effective memory capacity of the cluster (a more compact format fits more entries in the same memory bound) — worth choosing deliberately based on the actual payload sizes and access rates involved, rather than defaulting to whatever format is most convenient to read in a debugger.

### Memory fragmentation and the operational reality of long-running cache processes

```plaintext
Per this series' Memory Management guide: a cache process handling many
  variably-sized entries over a long uptime can suffer real memory
  fragmentation, where the process's total memory footprint grows even
  though the LOGICAL amount of cached data hasn't — a genuine operational
  concern distinct from the eviction policy (Section 5) governing what's kept.
```

As covered in this series' Memory Management guide, this is a real, if often underappreciated, operational concern for long-running cache nodes — worth monitoring (Section 14) memory fragmentation ratio directly, and worth knowing that a periodic, planned node restart or a memory-allocator tuned for this specific workload pattern (as most production cache systems, including Redis, provide) are the standard mitigations, rather than treating fragmentation as a mysterious, unaddressable memory leak.

---

## 11. Data Security and Multi-Tenancy

### Isolating tenants sharing the same cache cluster

```plaintext
Per this series' Multi-Tenancy guide: a shared cache cluster serving
  multiple tenants (in a SaaS deployment) needs key namespacing (prefixing
  every key with a tenant identifier) at minimum, and for genuinely
  sensitive data, LOGICAL or physical isolation (separate cache instances
  or partitions per tenant) may be warranted rather than relying on
  namespacing alone.
```

As covered in this series' Multi-Tenancy guide, a shared cache is a shared blast radius — a bug in key construction that omits the tenant prefix, or a noisy-neighbor tenant's hot keys (Section 8) affecting shared cluster capacity, are both real risks worth designing against deliberately rather than assuming namespacing alone fully solves tenant isolation.

### Encryption in transit, and the question of encryption at rest for cached data

```plaintext
TLS between clients and cache nodes, and between replicas (Section 4), is
  standard practice per this series' Secret Management and Transport
  Security guides — encryption AT REST for cache data is a genuine,
  deployment-specific question, since cache data is by definition
  regenerable from origin (Section 1), which changes the risk calculus
  somewhat compared to encrypting a primary data store.
```

Per this series' Secret Management guide, transport encryption is non-negotiable, but at-rest encryption for cache data specifically is worth a deliberate decision rather than a blanket default: because cached data is a derived, regenerable copy (Section 1), the actual sensitivity of what's cached (and the deployment's specific compliance requirements) should drive whether at-rest encryption's added overhead is warranted for a given cache cluster, rather than applying the same policy uniformly regardless of what's being cached.

### Never caching data whose freshness itself is a security property

```plaintext
Certain data — a revoked authorization, an updated permission set, a
  session invalidated after logout — has FRESHNESS as a genuine security
  property, not just a UX nicety, per this series' Authentication and
  Authorization guides — caching this kind of data needs Section 6's
  explicit or event-driven invalidation, never TTL-only staleness tolerance,
  regardless of how short the TTL is.
```

Worth flagging as a genuine exception to Section 1's general "bounded staleness is fine" framing: for data where staleness itself is a security risk (an access-revocation not yet reflected in a cached permission check), TTL-only invalidation is the wrong choice regardless of how short the window is — this class of data needs the explicit, write-path-coupled invalidation Section 6 describes, treated as a hard requirement rather than a latency optimization to skip under time pressure.

---

## 12. Consistency, Availability, and the CAP Trade-off for a Cache

### Why a cache almost always favors availability, given what it fundamentally is

As covered in this series' System Design guide's CAP theorem discussion, and consistent with Section 1's opening framing, a distributed cache is one of the clearest cases in this series' collection for favoring availability and eventual consistency by design, not as a compromise — the entire reason a cache exists is to serve fast, and a cache that sacrificed availability to guarantee perfect freshness would have given up the one property that justifies its existence.

### Where the trade-off genuinely shifts, echoing Section 11's security exception

```plaintext
Ordinary cached data (product details, rendered page fragments) → strongly
  favors availability, bounded staleness per Section 1
Security-relevant cached state (Section 11: permissions, session validity)
  → favors CONSISTENCY, even at some availability or latency cost, because
  staleness here isn't just inconvenient, it's a genuine security gap
```

This is the same per-data-type reasoning this series' Notification System guide applies to preference checks and this series' Rate Limiter guide applies to security-critical rules — most of a cache's data comfortably tolerates eventual consistency, but a narrow, identifiable slice (anything where "stale" means "potentially still granting access that's been revoked") deserves a deliberately stricter consistency posture, decided per data type rather than applied as one blanket policy across the whole cluster.

---

## 13. Scaling the System

### Applying this series' System Design guide's building blocks, with cache-specific emphasis

```plaintext
Sharding via consistent hashing (Section 3): the primary scaling lever for
  both memory capacity and request throughput, distributing both roughly
  evenly across the cluster as nodes are added
Read replicas (Section 4): scale read throughput independently of the
  partition count, particularly valuable given how read-heavy most caching
  workloads are by definition
Tiered caching (per this series' Multi-Layer Caching discussion): a small,
  very fast local (in-process) cache in front of the shared distributed
  cache, absorbing the hottest fraction of keys without even a network hop —
  directly echoing this series' URL Shortener guide's Section 13 multi-layer
  caching discussion, applied here as the caching layer's own internal architecture
```

Every technique from this series' System Design guide applies here, with the caveat that Section 12 already established this system tolerates staleness comfortably for most data, so — much like this series' URL Shortener guide's Section 13 — most of these techniques can be applied aggressively without the careful, scoped exceptions other capstone guides in this series required, aside from Section 11's narrow security-data carve-out.

### Capacity planning: sizing the cluster to the actual working set, not the full dataset

```plaintext
Per this series' Capacity Planning guide: a cache should be sized to hold
  the WORKING SET (the fraction of data actually accessed frequently
  enough to benefit from caching) with comfortable headroom, not the full
  origin dataset — oversizing wastes cost, undersizing causes excessive
  eviction churn (Section 5) that erodes the cache's hit rate and, with it,
  its entire reason for existing.
```

As covered in this series' Capacity Planning guide, correctly estimating the working set size — typically by analyzing real access-pattern data rather than guessing — is the single most consequential sizing decision for a cache cluster, since undersizing directly degrades the hit rate that determines whether the cache is doing its job at all.

---

## 14. Observability for a Distributed Cache

### Every guide in this series' observability trio, applied with hit-rate-specific stakes

```plaintext
Structured logs (per this series' Structured Logging guide): eviction
  events, invalidation events (Section 6), and node failover events
  (Section 9) — sampled for ordinary hits/misses given their sheer volume
Distributed tracing (per this series' Distributed Tracing guide): the
  cache lookup should appear as a clearly labeled span in traced requests,
  distinguishing hit-path latency from miss-path (origin fallback) latency
Metrics (per this series' Prometheus/Grafana guide): hit rate (per this
  series' URL Shortener guide's framing, the closest thing this system has
  to a one-number health summary), per-node memory utilization and
  fragmentation ratio (Section 10), eviction rate, replication lag
  (Section 4), and stampede-mitigation activation count (Section 8)
```

Every technique from this series' observability guides applies directly, echoing and extending this series' URL Shortener guide's own emphasis on cache hit rate as the system's single most important self-reported number — but here that number needs to be tracked per-node and per-key-pattern as well as in aggregate, since Section 8's hot-key problem means a healthy aggregate hit rate can still hide one specific, overwhelmed node.

### Alerting on hit-rate degradation and replication-health symptoms

```promql
# Per this series' Prometheus/Grafana guide's symptom-based alerting principle
(sum(rate(cache_hits_total[5m])) / sum(rate(cache_requests_total[5m]))) < 0.90
  or
max(replication_lag_seconds) > 5
```

A cache hit rate dropping below an established baseline, or replication lag (Section 4) growing beyond an acceptable bound, are exactly the kind of symptoms this series' Prometheus/Grafana guide argues alerts should be built around — the two are worth distinguishing clearly, since a hit-rate drop usually signals a working-set or eviction-policy problem (Section 5, Section 13) while growing replication lag signals infrastructure trouble that specifically widens Section 9's failover staleness window.

---

## 15. Common Pitfalls

| Pitfall | Why it hurts | Better approach |
|---|---|---|
| Naive `hash(key) % node_count` partitioning | Any scaling event remaps nearly every key, causing a system-wide cache-effectiveness collapse | Consistent hashing with virtual nodes, bounding remapping to a small fraction of keys per scaling event |
| No replication of any kind | A single node failure makes its entire slice of the keyspace unavailable and can trigger a stampede on the origin store | Leader-follower replication per partition, with asynchronous replication as the appropriate durability trade-off for regenerable cache data |
| TTL-only invalidation for security-relevant data | Stale permission/session data can grant access that's already been revoked, for the full TTL duration | Explicit or event-driven invalidation (Section 6) for any data where freshness is a security property, never TTL alone |
| No coordination on cache-miss stampedes for hot keys | A hot key's expiration can send a synchronized wave of requests to an origin store sized for cached traffic, overwhelming it | Request coalescing (single-flight) and probabilistic early expiration for hot keys |
| A single eviction policy applied without regard to actual access patterns | LRU can be fooled by a burst of one-off accesses evicting genuinely popular entries under an LFU-shaped workload | Choose LRU, LFU, or a hybrid deliberately, based on the real access-pattern distribution of the cached data |
| Treating cache-node memory growth as a mysterious leak | Long-running processes handling variably-sized entries can fragment memory even with a correct eviction policy in place | Monitor fragmentation ratio explicitly; use planned restarts or an allocator tuned for the workload |
| Sizing the cluster to the full origin dataset instead of the working set | Wastes cost without improving hit rate, since most of a full dataset is rarely accessed frequently enough to benefit from caching | Size to the actual working set, determined from real access-pattern analysis, with headroom |
| No tenant key namespacing in a shared multi-tenant cache | A key-construction bug or a noisy-neighbor tenant's hot keys can leak data or degrade capacity across tenant boundaries | Explicit tenant-prefixed keys at minimum; logical or physical isolation for genuinely sensitive tenant data |

---

## Quick Reference Table

| Concept | Purpose |
|---|---|
| Consistent hashing (with virtual nodes) | Bounds key remapping during scaling events, preventing a system-wide cache-effectiveness collapse |
| Leader-follower replication, asynchronous | Provides failover and read scaling, accepting a small bounded staleness window appropriate for regenerable data |
| Deliberate eviction policy (LRU/LFU/hybrid) | Matches eviction behavior to the cached data's actual access-pattern distribution |
| TTL as a complementary, not alternative, mechanism to size-based eviction | Bounds maximum staleness independent of memory pressure |
| Explicit or event-driven invalidation | Closes the staleness gap TTL alone leaves open, especially for security-relevant data |
| Cache-aside as the default read/write pattern | Requires no special cache support and composes cleanly with any invalidation strategy |
| Request coalescing + probabilistic early expiration | Prevents cache stampedes from overwhelming an origin store sized only for cached traffic |
| Working-set-based capacity planning | Sizes the cluster to what actually needs caching, protecting hit rate as the system's core health signal |

---

## Conclusion

A distributed cache takes every general system design technique covered throughout this series and applies it to a problem whose central difficulty is a famously, genuinely hard one — because the entire value of caching rests on serving stale-but-acceptable data fast, and "acceptable" is a boundary that has to be actively, deliberately managed rather than assumed. The design that actually holds up rests on a small number of foundational decisions: consistent hashing to keep partitioning changes from becoming cache-wide effectiveness collapses; replication that deliberately favors availability over durability, matched to what cache data actually is; an eviction policy chosen for the real access pattern rather than defaulted blindly; invalidation strategies scoped to how much staleness each specific kind of data can actually tolerate — including a hard, non-negotiable exception for anything where freshness is itself a security property; and stampede protection that keeps a hot key's expiration from becoming an accidental denial-of-service against the very origin store the cache exists to protect.

Nearly every architectural pattern covered elsewhere in this series shows up here in service of that bar — Consistent Hashing and Redis's atomic operations as shared foundations with this series' Rate Limiter and High-Volume Transaction Processing guides, Event-Driven Architecture's decoupled invalidation echoing this series' Notification System guide's fan-out pattern, and the full observability trio watching hit rate as this system's own single most telling number, in the same way this series' URL Shortener guide frames it. A distributed cache is, in that sense, less a distinct discipline from everything else in this series than the place where its cumulative lessons about bounded staleness, deliberate trade-offs, and honest management of a famously hard problem come together most directly and most instructively.

---

*Found this useful? Feel free to star the repo, open an issue with corrections, or share the hot-key-expired-and-took-down-the-database stampede incident that turned out to matter far more than an aggregate hit-rate dashboard ever revealed on its own.*
