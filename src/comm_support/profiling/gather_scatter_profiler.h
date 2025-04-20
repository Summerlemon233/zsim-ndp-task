#pragma once
#include "locks.h"
#include "config.h"

namespace pimbridge {

class GatherScatterProfiler {
private:
    class BwUtilEntry {
    public: 
        uint32_t sum;
        std::vector<uint32_t> transferSizes;
        uint32_t maxNumChild = 32; 
        BwUtilEntry() : sum(0) {
            transferSizes.resize(maxNumChild, 0); 
        }
        void recordTransfer(uint32_t idx, uint32_t size) {
            assert(idx < maxNumChild);
            this->transferSizes[idx] += size;
            this->sum += size;
        }
    };
    bool enableTrace;
    lock_t lock;
    std::string pathPrefix;
    // level, commModule, each operation
    std::vector<std::vector<std::vector<BwUtilEntry>>> bwUtil;
    std::vector<std::vector<std::vector<uint64_t>>> intervalLength;
public:
    GatherScatterProfiler(Config& config, const std::string& prefix);
    GatherScatterProfiler() : enableTrace(false) {}
    void initTransfer(uint32_t level, uint32_t commId);
    void record(uint32_t level, uint32_t commId, uint32_t childIdx, uint32_t size);
    void toFile();
private:
    void bwUtilToFile();
};


}