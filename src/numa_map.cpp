#include <bitset>
#include "numa_map.h"
#include "constants.h"
#include "g_std/g_map.h"
#include "g_std/g_unordered_map.h"
#include "locks.h"
#include "scheduler.h"
#include "zsim.h"  // for getCid

//#define DEBUG(args...) info(args)
#define DEBUG(args...)

/* Thread-safe, bucket-based page-to-node map. */
class PageMap : public GlobAlloc {
    public:
        PageMap() {
            futex_init(&futex);
        }

        bool isPresent(const Address pageAddr) {
            auto chunk = findChunk(pageAddr);
            return chunk && chunk->isPresent(pageAddr);
        }

        uint32_t get(const Address pageAddr) {
            auto chunk = findChunk(pageAddr);
            return chunk ? chunk->lookup(pageAddr) : NUMAMap::INVALID_NODE;
        }

        size_t add(Address pageAddr, size_t pageCount, const uint32_t node) {
            if (pageCount == 0) return 0;
            if (node == NUMAMap::INVALID_NODE) return pageCount;
            DEBUG("[PageMap] add %lx (%lu) -> %u", pageAddr, pageCount, node);
            size_t ignoredCount = 0;
            while (pageCount > 0) {
                Address nextPageAddr = nextChunkPageAddr(pageAddr);
                size_t cnt = std::min(pageCount, nextPageAddr - pageAddr);
                ignoredCount += findChunk(pageAddr, true)->add(pageAddr, cnt, node);
                pageCount -= cnt;
                pageAddr = nextPageAddr;
            }
            return ignoredCount;
        }

        void remove(Address pageAddr, size_t pageCount) {
            if (pageCount == 0) return;
            DEBUG("[PageMap] remove %lx (%lu)", pageAddr, pageCount);
            while (pageCount > 0) {
                Address nextPageAddr = nextChunkPageAddr(pageAddr);
                size_t cnt = std::min(pageCount, nextPageAddr - pageAddr);
                auto chunk = findChunk(pageAddr);
                if (chunk) chunk->remove(pageAddr, cnt);
                pageCount -= cnt;
                pageAddr = nextPageAddr;
            }
        }

    private:
        static constexpr uint32_t CHUNK_BITS = 16;  // 2^16 pages, i.e., 256 MB

        struct PageRange {
            Address pageAddrBegin;
            Address pageAddrEnd;
            uint32_t node;

            // We need to keep the removed pages around. The remove happens at unmap time, but the data may not be
            // evicted from caches until later. We still need the map for writeback.
            // Pages marked as removed will be silently overwritten by newly added pages to the same place.
            bool removed;

            PageRange(const Address _pageAddr, const size_t _pageCount, const uint32_t _node, bool _removed = false)
                : pageAddrBegin(_pageAddr), pageAddrEnd(_pageAddr + _pageCount), node(_node), removed(_removed) {}

            PageRange() : PageRange(0, 0, NUMAMap::INVALID_NODE) {}

            inline bool contains(const Address pageAddr) const {
                return (pageAddr >= pageAddrBegin) && (pageAddr < pageAddrEnd);
            }

            inline size_t count() const {
                return pageAddrEnd - pageAddrBegin;
            }

            inline bool tryMergeWith(const PageRange& other) {
                if (node == other.node && removed == other.removed
                        && (pageAddrEnd >= other.pageAddrBegin && other.pageAddrEnd >= pageAddrBegin)) {
                    auto cnt = count() + other.count();
                    pageAddrBegin = std::min(pageAddrBegin, other.pageAddrBegin);
                    pageAddrEnd = std::max(pageAddrEnd, other.pageAddrEnd);
                    assert(count() <= cnt);
                    return true;
                }
                return false;
            }
        };

        class PageChunk {
        private:
            static constexpr uint64_t CHUNK_SIZE = 1 << PageMap::CHUNK_BITS;
            static constexpr Address CHUNK_MASK = (CHUNK_SIZE - 1);

            using ChunkStorage = g_map<Address, PageRange>;  // use page range begin address as key

            ChunkStorage chunk;
            std::bitset<CHUNK_SIZE> pmap;
            lock_t futex;

        public:
            PageChunk() {
                futex_init(&futex);
            }

            bool isPresent(Address pageAddr) {
                // Check bitmap.
                futex_lock(&futex);
                bool p = pmap[pageAddr & CHUNK_MASK];
                futex_unlock(&futex);
                return p;
            }

            uint32_t lookup(Address pageAddr) {
                // Look up in chunk to get the node for the page.
                uint32_t node = NUMAMap::INVALID_NODE;
                futex_lock(&futex);
                auto it = findBefore(pageAddr);
                if (it->second.contains(pageAddr)) {
                    node = it->second.node;
                }
                futex_unlock(&futex);
                return node;
            }

            size_t add(Address pageAddr, size_t pageCount, const uint32_t node) {
                // Add to chunk and keep the order, and merge if necessary.
                // Return number of pages that already exist and thus are ignored.
                size_t ignoredCount = 0;
                auto newpr = PageRange(pageAddr, pageCount, node);
                futex_lock(&futex);
                for (size_t i = 0; i < pageCount; i++) {
                    pmap.set((pageAddr & CHUNK_MASK) + i);
                }
                auto it = findBefore(pageAddr);
                while (it != chunk.end() && newpr.count()) {
                    auto& pr = it->second;
                    if (newpr.pageAddrEnd < pr.pageAddrBegin) {
                        // No overlap with following ones, insert as new entry.
                        safeInsertBefore(it, newpr);
                    } else if (newpr.pageAddrBegin > pr.pageAddrEnd) {
                        // No overlap but after the current one, do nothing.
                        ++it;
                    } else if (pr.removed) {
                        // Overlaps with current one which has been removed.
                        // Overwrite the overlapped part.

                        // Split the current range.
                        PageRange before, overlap, after;
                        split(pr, newpr, before, overlap, after);
                        // Remove it.
                        chunk.erase(it++);

                        // If there is non-overlap range before the new range ...
                        if (before.count()) {
                            // Add back non-overlap range before the new range.
                            safeInsertBefore(it, before);
                        } else {
                            // ... otherwise, try merge with the previous neighbor.
                            if (it != chunk.begin()) {
                                auto prev = it;
                                --prev;
                                if (newpr.tryMergeWith(prev->second)) {
                                    chunk.erase(prev);
                                }
                            }
                        }

                        // If there is non-overlap range after the new range ...
                        if (after.count()) {
                            // Insert the new range.
                            safeInsertBefore(it, newpr);

                            // Add back non-overlap range after the new range.
                            safeInsertBefore(it, after);
                        }
                        // ... otherwise, the current removed range is completely handled.
                        // We leave the new range unchanged to compare with the next one.
                    } else if (newpr.node == pr.node) {
                        // Overlaps with current one and the same node.
                        // Remove the current one and merge into the new range.
                        assert(newpr.tryMergeWith(pr));
                        chunk.erase(it++);
                    } else {
                        // Overlaps with current one but different nodes.
                        // Insert the non-overlapped part.

                        // Split the new range.
                        PageRange before, overlap, after;
                        split(newpr, pr, before, overlap, after);

                        // Overlap range is ignored.
                        ignoredCount += overlap.count();

                        // Insert non-overlap range before the current one.
                        if (before.count()) {
                            safeInsertBefore(it, before);
                        }

                        // Use non-overlap range after the current one as the new one.
                        newpr = after;

                        ++it;
                    }
                }
                // Push to the end.
                if (newpr.count()) safeInsertBefore(it, newpr);
                verify();
                futex_unlock(&futex);
                return ignoredCount;
            }

            void remove(Address pageAddr, size_t pageCount) {
                // Remove lazily from chunk, and split if necessary.
                auto rempr = PageRange(pageAddr, pageCount, NUMAMap::INVALID_NODE, true);
                futex_lock(&futex);
                for (size_t i = 0; i < pageCount; i++) {
                    pmap.set((pageAddr & CHUNK_MASK) + i, false);
                }
                auto it = findBefore(pageAddr);
                while (it != chunk.end()) {
                    auto& pr = it->second;
                    if (rempr.pageAddrEnd < pr.pageAddrBegin) {
                        // No overlap with following ones, return.
                        break;
                    } else if (rempr.pageAddrBegin > pr.pageAddrEnd) {
                        // No overlap but after the current one, continue.
                        ++it;
                    } else if (!pr.removed) {
                        // Overlaps with current one which is not removed.
                        // Mark the overlapped part as removed.

                        // Split the current range.
                        PageRange before, overlap, after;
                        split(pr, rempr, before, overlap, after);
                        // Remove it.
                        chunk.erase(it++);

                        // Add back non-overlap range before the removing range.
                        if (before.count()) {
                            safeInsertBefore(it, before);
                        }

                        // Add back overlap range as removed.
                        if (overlap.count()) {
                            assert(!overlap.removed);
                            overlap.removed = true;

                            // Try merge with removed neighbors.
                            if (it != chunk.begin()) {
                                auto prev = it;
                                --prev;
                                if (overlap.tryMergeWith(prev->second)) {
                                    chunk.erase(prev);
                                }
                            }
                            if (it != chunk.end()) {
                                if (overlap.tryMergeWith(it->second)) {
                                    chunk.erase(it++);
                                }
                            }
                            safeInsertBefore(it, overlap);
                        }

                        // Add back non-overlap range after the removing range.
                        if (after.count()) {
                            safeInsertBefore(it, after);
                        }
                    } else {
                        // Overlaps with current one which has been removed, do nothing.
                        ++it;
                    }
                }
                verify();
                futex_unlock(&futex);
            }

            void verify() const {
#if 0
                uint64_t lastPageAddrEnd = 0;
                uint64_t lastNode = NUMAMap::INVALID_NODE;
                bool lastRemoved = false;
                for (auto it = chunk.begin(); it != chunk.end(); ++it) {
                    const auto& pr = it->second;
                    DEBUG("[PageMap] verify 0x%lx-0x%lx -> %u%s",
                            pr.pageAddrBegin, pr.pageAddrEnd, pr.node, pr.removed ? " D" : "");
                    assert(pr.pageAddrBegin < pr.pageAddrEnd);
                    assert(pr.node != NUMAMap::INVALID_NODE);
                    if (pr.node == lastNode && !pr.removed && !lastRemoved) {
                        assert_msg(pr.pageAddrBegin > lastPageAddrEnd, "page ranges overlap");
                    } else {
                        assert_msg(pr.pageAddrBegin >= lastPageAddrEnd, "page ranges overlap");
                    }
                    lastPageAddrEnd = pr.pageAddrEnd;
                    lastNode = pr.node;
                    lastRemoved = pr.removed;
                }
#endif
            }

        private:
            /* Get an iterator to a range (if any) whose begin address is before the given address.
             * If the first range is after the address, return the begin iterator.
             * If the address is after all ranges, return the last valid iterator.
             */
            inline ChunkStorage::iterator findBefore(Address pageAddr) {
                auto it = chunk.upper_bound(pageAddr);
                assert_msg(it == chunk.end() || it->first > pageAddr, "upper_bound." /* to workaround operator<< on iterator type. */);
                if (it != chunk.begin()) {
                    --it;
                    assert(it->first <= pageAddr);
                }
                return it;
            }

            inline void safeInsertBefore(ChunkStorage::iterator& next, PageRange& e) {
                // auto it = chunk.insert(next /* hint */, std::make_pair<Address, PageRange>(e.pageAddrBegin, e));
                auto it = chunk.insert(next /* hint */, std::make_pair(e.pageAddrBegin, e));
                e.pageAddrEnd = e.pageAddrBegin;  // clear the inserted range.
                assert(it->second.count() > 0);  // not affected
                if (next != chunk.end()) {
                    assert(it->second.pageAddrEnd <= next->second.pageAddrBegin);
                }
                if (it != chunk.begin()) {
                    auto prev = it;
                    --prev;
                    assert(it->second.pageAddrBegin >= prev->second.pageAddrEnd);
                }
            }

            inline void split(const PageRange& orig, const PageRange& splitter,
                    PageRange& before, PageRange& overlap, PageRange& after) {
                auto makeRange = [orig](Address begin, Address end) -> PageRange {
                    return PageRange(begin, begin < end ? end - begin : 0, orig.node, orig.removed);
                };
                before = makeRange(orig.pageAddrBegin, splitter.pageAddrBegin);
                overlap = makeRange(std::max(orig.pageAddrBegin, splitter.pageAddrBegin), std::min(orig.pageAddrEnd, splitter.pageAddrEnd));
                after = makeRange(splitter.pageAddrEnd, orig.pageAddrEnd);
                assert(before.count() + overlap.count() + after.count() == orig.count());
            }
        };

        g_unordered_map<uint64_t, PageChunk> pageMaps;

        lock_t futex;

    private:
        PageChunk* findChunk(const Address pageAddr, bool insert = false) {
            uint64_t chunkIdx = pageAddr >> CHUNK_BITS;
            PageChunk* chunk = nullptr;
            futex_lock(&futex);
            if (insert) {
                chunk = &pageMaps[chunkIdx];
            } else {
                auto it = pageMaps.find(chunkIdx);
                if (it != pageMaps.end()) {
                    chunk = &it->second;
                }
            }
            futex_unlock(&futex);
            return chunk;
        }

        inline static Address nextChunkPageAddr(const Address pageAddr) {
            uint64_t chunkIdx = pageAddr >> CHUNK_BITS;
            return (chunkIdx + 1) << CHUNK_BITS;
        }
};


constexpr uint32_t NUMAMap::INVALID_NODE;

NUMAMap::NUMAMap(const char* _patchRoot, const uint32_t numCores)
    : patchRoot(_patchRoot == nullptr ? "" : _patchRoot),
      coreNodeMap(numCores, INVALID_NODE), pageNodeMap(new PageMap())
{
    // Use patched root to figure out NUMA core map.
    if (patchRoot.empty()) panic("NUMA needs to patch the root path in the main process!");
    uint32_t node = 0;
    while (parseBitmap(node) == 0 && node < 1024 /*avoid inf loop*/) { node++; }
    maxNode = node - 1;
    for (uint32_t cid = 0; cid < numCores; cid++) {
        if (coreNodeMap[cid] == INVALID_NODE) {
            warn("Core %u has no associated NUMA node", cid);
        }
    }

    futex_init(&lock);
}

uint32_t NUMAMap::getNodeOfPage(const Address pageAddr) {
    auto node = pageNodeMap->get(pageAddr);
    assert_msg(node != INVALID_NODE, "Page addr %lx has not been allocated!", pageAddr);
    return node;
}

uint32_t NUMAMap::getNodeOfPageSafe(const Address pageAddr) {
    if (unlikely(!pageNodeMap->isPresent(pageAddr))) {
        return INVALID_NODE;
    }
    return pageNodeMap->get(pageAddr);
}

void NUMAMap::allocateFromCore(const Address addr, const uint32_t cid) {
    auto pageAddr = getPageAddress(addr);
    if (unlikely(!pageNodeMap->isPresent(pageAddr))) {
        assert(cid < zinfo->numCores);
        uint32_t pid = zinfo->sched->getScheduledPid(cid);
        uint32_t tid = zinfo->sched->getScheduledTid(cid);
        assert_msg(pid != -1u && tid != -1u, "Core %u has no thread running! Who is allocating the line?", cid);
        addPagesThreadPolicy(pageAddr, 1, pid, tid, cid);  // adding pages could race
        assert(pageNodeMap->isPresent(pageAddr));
    }
}

size_t NUMAMap::addPagesToNode(const Address pageAddr, const size_t pageCount, const uint32_t node) {
    return pageNodeMap->add(pageAddr, pageCount, node);
}

void NUMAMap::removePages(const Address pageAddr, const size_t pageCount) {
    pageNodeMap->remove(pageAddr, pageCount);
}

size_t NUMAMap::addPagesThreadPolicy(const Address pageAddr, const size_t pageCount, const uint32_t pid, const uint32_t tid, const uint32_t cid, NUMAPolicy* policy) {
    if (!policy) {
        // Use the policy of the thread.
        uint64_t gid = (((uint64_t)pid) << 32) | tid;
        futex_lock(&lock);
        policy = &threadPolicy[gid];
        futex_unlock(&lock);
    }
    const auto& mode = policy->getMode();

    size_t ignoredCount = 0;
    bool success = false;

    // See Linux doc set_mempolicy(2).
    if (mode == MPOL_DEFAULT
#ifdef MPOL_LOCAL
            || mode == MPOL_LOCAL
#endif  // MPOL_LOCAL
       ) {
        // Local allocation.
        success = tryAddPagesLocal(pageAddr, pageCount, getNodeOfCore(cid), false, ignoredCount);
    } else if (mode == MPOL_PREFERRED) {
        // Preferred node allocation.
        // The preferred node is the first node in nodemask.
        uint32_t node = 0;
        while (node <= maxNode && !policy->isAllowed(node)) node++;
        if (node > maxNode) {
            // Empty nodemask. Fall back to default policy.
            node = getNodeOfCore(cid);
        }
        success = tryAddPagesLocal(pageAddr, pageCount, node, false, ignoredCount);
    } else if (mode == MPOL_BIND) {
        // Strict bind allocation.
        for (uint32_t node = 0; node <= maxNode; node++) {
            if (!policy->isAllowed(node)) continue;
            success = tryAddPagesLocal(pageAddr, pageCount, node, true, ignoredCount);
            if (success) break;
        }
    } else if (mode == MPOL_INTERLEAVE) {
        // Interleaving allocation.
        // Interleave across the allowed nodes.
        success = tryAddPagesInterleaved(pageAddr, pageCount, policy, ignoredCount);
    } else {
        panic("Invalid NUMA policy mode %d", mode);
    }

    if (!success) panic("NUMA allocation fails. Thread %u, mode %d, page count %lu", tid, mode, pageCount);
    assert(ignoredCount <= pageCount);
    return ignoredCount;
}

int NUMAMap::parseBitmap(const uint32_t node) {
    // Open cpumap file.
    size_t fnLen = patchRoot.size() + 128;
    char* fname = new char[fnLen];
    snprintf(fname, fnLen, "%s/sys/devices/system/node/node%u/cpumap", patchRoot.c_str(), node);
    FILE* f = fopen(fname, "r");
    delete[] fname;
    if (!f) return -1;

    // Read bitmap string.
    char* line = nullptr;
    size_t len = 0;
    int ret = getline(&line, &len, f);
    fclose(f);
    if (ret < 0) return -1;

    // Parse bitmap string.
    char* p = strchr(line, '\n');
    if (!p) {
        free(line);
        return -1;
    }
    for (uint32_t i = 0; p > line; i++) {
        char* oldp = p;
        if (*p == ',') --p;
        while (p > line && *p != ',') --p;
        if (*p == ',') p++;
        // Now p points to the first number, oldp points to the char after the last number.
        // There are 8 chars between p and oldp, corresponding to a 32-bit mask.
        assert(oldp - p == 8);
        char* endp;
        uint32_t mask = strtoul(p, &endp, 16);
        if (endp != oldp) {
            free(line);
            return -1;
        }
        p--;

        // Update core-to-node map.
        if (!mask) continue;
        for (uint32_t bit = 0; bit < 32; bit++) {
            bool isset = (mask >> bit) & 0x1;
            if (isset) {
                uint32_t cid = i * 32 + bit;
                assert_msg(cid < coreNodeMap.size(),
                        "There is more cores in patched root (>= %u) than in system config (%lu)",
                        cid, coreNodeMap.size());
                assert_msg(coreNodeMap[cid] == INVALID_NODE,
                        "Core %u belongs to multiple NUMA node %u and %u",
                        cid, coreNodeMap[cid], node);
                coreNodeMap[cid] = node;
            }
        }
    }

    free(line);
    return 0;
}

bool NUMAMap::tryAddPagesLocal(const Address pageAddr, const size_t pageCount, uint32_t node, bool strict, size_t& ignoredCount) {
    if (node == INVALID_NODE) {
        // This core does not belong to any node, i.e., memory-less node.
        // Interleave the allocation across all nodes.
        assert_msg(!strict, "Local allocation cannot be strict for memory-less node.");
        NUMAPolicy ip(MPOL_INTERLEAVE, g_vector<bool>(maxNode + 1, true));
        return tryAddPagesInterleaved(pageAddr, pageCount, &ip, ignoredCount);
    }

    if (strict) {
        // TODO: fail if not enough memory.
        ignoredCount += addPagesToNode(pageAddr, pageCount, node);
        return true;
    }

    // Not strict, also try nearby nodes.
    // We try the next node one by one.
    for (uint32_t t = 0; t <= maxNode; t++) {
        if (tryAddPagesLocal(pageAddr, pageCount, node, true, ignoredCount)) return true;
        node = (node + 1) % (maxNode + 1);
    }
    return false;
}

bool NUMAMap::tryAddPagesInterleaved(const Address pageAddr, const size_t pageCount, NUMAPolicy* policy, size_t& ignoredCount) {
    assert(policy && policy->getMode() == MPOL_INTERLEAVE);
    size_t igcnt = 0;
    for (size_t pa = pageAddr; pa < pageAddr + pageCount; pa++) {
        bool success = false;
        for (size_t t = 0; t <= maxNode && !success; t++) {
            success = tryAddPagesLocal(pa, 1, policy->updateNext(), true, igcnt);
        }
        if (!success) return false;
    }
    assert(igcnt <= pageCount);
    ignoredCount += igcnt;
    return true;
}

