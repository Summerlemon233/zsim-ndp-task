#pragma once
#include <cstdint>
#include <iostream>
#include <vector>
#include <deque>
#include <unordered_map>
#include <queue>
#include "config.h"
#include "memory_hierarchy.h"

namespace pimbridge {

class DataHotness {
public:
    Address addr;
    uint32_t srcBankId;
    uint32_t cnt;
    DataHotness(Address _addr, uint32_t _srcBankId, uint32_t _cnt) : 
        addr(_addr), srcBankId(_srcBankId), cnt(_cnt){}
    void reset() {
        addr = 0;
        cnt = 0;
    }
};

/* Each LbCommand targets a single bank (taskUnit)
 * The values in perStealerCommand refers to the number of tasks 
   that should be stolen by each stealer. 
 * For example, bank-1 and bank-2 needs to steal 5 tasks and 7 tasks from bank-0, 
   then the LbCommand for bank-0 is (5, 7)
 */
class LbCommand {
private:
    std::vector<uint32_t> perStealerCommand;
public:
    LbCommand() {}
    void reset() { this->perStealerCommand.clear(); }
    void add(uint32_t c) { this->perStealerCommand.push_back(c); }
    const std::vector<uint32_t>& get() const { return this->perStealerCommand; }
    bool empty() const { return perStealerCommand.empty(); }
    std::string output() { 
        if (perStealerCommand.empty()) { return "None"; }
        std::stringstream ss;
        for (uint32_t c : perStealerCommand) {
            ss << c << " "; 
        }
        std::string res = ss.str();
        return res;
    }
};

class CommModule;

// The load balancer give commands for children to execute. 
// The commands are integers, indicating the number of tasks that should be scheduled out.
class LoadBalancer {
protected:
    bool DYNAMIC_THRESHOLD;
    uint32_t STEALER_THRESHOLD;
    uint32_t VICTIM_THRESHOLD;
    enum ChunkScheme {
        Static, 
        Dynamic, 
        HalfVictim
    };
    ChunkScheme chunkScheme;
    uint32_t CHUNK_SIZE;

    uint32_t level;
    uint32_t commId;
    CommModule* commModule;

    std::vector<LbCommand> commands; // commands for each bank
    std::vector<bool> canDemand; // in this level, whether a bank can steal from any other banks

    std::vector<uint32_t> demand;
    std::vector<uint32_t> supply;
    std::vector<uint32_t> remainTransfer;
    std::vector<uint32_t> demandIdxVec; // all demands, each is a pair<bankChildId, demandVal>
    std::vector<uint32_t> supplyIdxVec; // all supplies, each is a pair<bankChildId, supplyVal>

    std::vector<std::deque<std::pair<uint32_t, uint32_t>>> assignTable; // for each victim, which are its stealers and how much to steal
public: 
    LoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
    virtual void generateCommand(bool* needParentLevelLb) = 0;
    virtual void assignLbTarget(const std::vector<DataHotness>& outInfo) = 0;
    void setDynamicLbConfig();
protected:
    void assignOneAddr(Address addr, uint32_t target);
    void reset();
    void outputCommand();
    void outputDemandSupply();

    friend class CommModule;
};

// The StealingLoadBalancer schedule tasks from the tail of the task queue
// The commands generation is the same to behaviors of work stealing
class StealingLoadBalancer : public LoadBalancer {
public:
    StealingLoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
    void generateCommand(bool* needParentLevelLb) override;
    void assignLbTarget(const std::vector<DataHotness>& outInfo) override;
protected:
    virtual bool genDemand(uint32_t bankIdx); // how much to steal for each bank
    virtual bool genSupply(uint32_t bankIdx); // how much can be stolen for each bank
    uint32_t genScheduleAmount(uint32_t stealerIdx, uint32_t victimIdx);
};

class MultiVictimStealingLoadBalancer : public StealingLoadBalancer {
private:
    uint32_t victimNumber;
public:
    MultiVictimStealingLoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
    void generateCommand(bool* needParentLevelLb) override;
};

class ReserveLbPimBridgeTaskUnit;

class ReserveLoadBalancer : public StealingLoadBalancer {
protected:
    uint32_t HOT_DATA_NUMBER = 10;
    std::vector<DataHotness> childDataHotness;
public:
    ReserveLoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
    void generateCommand(bool* needParentLevelLb) override;
protected: 
    bool genSupply(uint32_t bankIdx) override;
};

class TryReserveLoadBalancer : public ReserveLoadBalancer {
public:
    TryReserveLoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
    void generateCommand(bool* needParentLevelLb) override;
};

class FastArriveLoadBalancer : public StealingLoadBalancer {
private:
public:
    FastArriveLoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
protected:
    bool genSupply(uint32_t bankIdx) override; 
};


}