#ifndef NUMA_MAP_H_
#define NUMA_MAP_H_

#include <numaif.h>
#include "bithacks.h"
#include "galloc.h"
#include "g_std/g_string.h"
#include "g_std/g_unordered_map.h"
#include "g_std/g_vector.h"
#include "locks.h"
#include "memory_hierarchy.h"
#include "process_local_val.h"
// #include "zsim.h"  // for lineBits

class NUMAPolicy : public GlobAlloc {
    public:
        NUMAPolicy(const int _mode, const g_vector<bool>& _mask) : mode(_mode), mask(_mask) {
            next = 0;
            if (mode == MPOL_INTERLEAVE) {
                // Set to the first node.
                while (next < getNodeCount() && !isAllowed(next)) next++;
                assert_msg(next < getNodeCount(), "MPOL_INTERLEAVE nodemask must be non-empty.");
            }
        }

        // Default policy.
        NUMAPolicy() : NUMAPolicy(MPOL_DEFAULT, g_vector<bool>()) {}

        int getMode() const { return mode; }

        const g_vector<bool>& getMask() const { return mask; }

        bool isAllowed(const uint32_t node) const { return node < mask.size() ? mask[node] : false; }

        uint32_t getNext() const {
            assert_msg(mode == MPOL_INTERLEAVE, "Next node to allocate is only valid for MPOL_INTERLEAVE.");
            return next;
        }

        // Return the next node to allocate and update.
        uint32_t updateNext() {
            auto cur = getNext();
            do { next = (next + 1) % getNodeCount(); } while (!isAllowed(next));
            return cur;
        }

        // Use glob mem.
        using GlobAlloc::operator new;
        using GlobAlloc::operator delete;

    private:
        int mode;
        g_vector<bool> mask;

        // The next node of interleaving allocation.
        uint32_t next;

        uint32_t getNodeCount() const { return mask.size(); }
};

class PageMap;

class NUMAMap : public GlobAlloc {
    public:
        static constexpr uint32_t INVALID_NODE = -1u;

    public:
        NUMAMap(const char* _patchRoot, const uint32_t numCores);

        uint32_t getMaxNode() const {
            return maxNode;
        }

        uint32_t getNodeOfCore(const uint32_t cid) const {
            return coreNodeMap[cid];
        }

        uint32_t getNodeOfLineAddr(const Address lineAddr) {
            return getNodeOfPage(lineAddr >> (pageBits - lineBits));
        }

        uint32_t getNodeOfPage(const Address pageAddr);
        
        // Safe version that returns INVALID_NODE if page is not allocated
        uint32_t getNodeOfPageSafe(const Address pageAddr);

        inline Address getPageAddress(const Address addr) {
            // NOTE: this must be equivalent to vAddr -> pLineAddr logic in filter_cache.h.
            return (addr >> pageBits) | (procMask >> (pageBits - lineBits));
        }

        inline Address getPageAddressFromLbPageAddress(const Address lbPageAddr) {
            return (lbPageAddr >> (pageBits - lbPageBits));
        }

        inline Address getNodeFromLbPageAddress(const Address lbPageAddr) {
            return this->getNodeOfPage(this->getPageAddressFromLbPageAddress(lbPageAddr));
        }

        inline Address getLbPageAddress(const Address addr) {
            return (addr >> lbPageBits) | (procMask >> (lbPageBits - lineBits));
        }

        // Allocate an address from a core if not yet allocated, use the policy of the thread running on the core.
        void allocateFromCore(const Address addr, const uint32_t cid);

        // Add given pages to NUMA node. Return the pages that already exist and thus are ignored.
        size_t addPagesToNode(const Address pageAddr, const size_t pageCount, const uint32_t node);
        // Remove given pages from NUMA map.
        void removePages(const Address pageAddr, const size_t pageCount);

        // Add given pages according to the policy, from the thread running on the core. If no policy is given, use the policy of the thread.
        // Return the pages that already exist and thus are ignored.
        // NOTE: when called inside a syscall, the thread has left the core, so we need to specify both tid and cid.
        size_t addPagesThreadPolicy(const Address pageAddr, const size_t pageCount, const uint32_t pid, const uint32_t tid, const uint32_t cid, NUMAPolicy* policy = nullptr);

        // Get the NUMA policy for the thread. Record and return default policy if absent.
        NUMAPolicy getThreadPolicy(const uint32_t pid, const uint32_t tid) {
            uint64_t gid = (((uint64_t)pid) << 32) | tid;
            futex_lock(&lock);
            auto policy = threadPolicy[gid];
            futex_unlock(&lock);
            return policy;
        }

        // Set the NUMA policy for the thread.
        void setThreadPolicy(const uint32_t pid, const uint32_t tid, const int mode, const g_vector<bool>& mask) {
            assert(mask.size() == maxNode + 1);
            uint64_t gid = (((uint64_t)pid) << 32) | tid;
            futex_lock(&lock);
            threadPolicy[gid] = NUMAPolicy(mode, mask);
            futex_unlock(&lock);
        }

        // Get the next NUMA node of interleaving allocation for the thread.
        uint32_t getThreadNextAllocNode(const uint32_t pid, const uint32_t tid) {
            uint64_t gid = (((uint64_t)pid) << 32) | tid;
            futex_lock(&lock);
            uint32_t next = threadPolicy.at(gid).getNext();
            futex_unlock(&lock);
            return next;
        }

        // Use glob mem.
        using GlobAlloc::operator new;
        using GlobAlloc::operator delete;

    private:
        // Max node, assuming nodes are continuous.
        uint32_t maxNode;

        /* NUMA core map. */

        // Patched root path to provide the NUMA node map info.
        const g_string patchRoot;
        // Core-to-node map.
        g_vector<uint32_t> coreNodeMap;

        /* NUMA memory map. */

        // Page-to-node map.
        PageMap* pageNodeMap;

        /* Thread NUMA policy. */

        g_unordered_map<uint64_t, NUMAPolicy> threadPolicy;  // indexed by ((pid << 32) | tid)
        lock_t lock;

    private:
        // Parse the character string found in /sys/devices/system/node/nodeN/cpumap
        // to initialize core-to-node map.
        // Implemented after lib numactl-2.0.11 libnuma.c:numa_parse_bitmap_v2().
        int parseBitmap(const uint32_t node);

        // NUMA "local allocation". See Linux doc set_mempolicy(2).
        // If strict, do not consider nearby nodes.
        // Return if success. Also update ignored page counts only if success.
        bool tryAddPagesLocal(const Address pageAddr, const size_t pageCount, uint32_t node, bool strict, size_t& ignoredCount);

        // Interleave the page allocation across the allowed NUMA nodes. Update the next node to allocate in the policy.
        // Return if success. Also update ignored page counts only if success.
        bool tryAddPagesInterleaved(const Address pageAddr, const size_t pageCount, NUMAPolicy* policy, size_t& ignoredCount);
};

#endif  // NUMA_MAP_H_

