#pragma once
#include <cstdint>
#include <vector>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include "galloc.h"
#include "stats.h"
#include "locks.h"
#include "config.h"
#include "task_support/task.h"
#include "comm_support/comm_packet.h"
#include "comm_support/comm_packet_queue.h"
#include "comm_support/gather_scheme.h"
#include "comm_support/scatter_scheme.h"
#include "load_balancing/load_balancer.h"
#include "load_balancing/bank_group_load_balancer.h"
#include "load_balancing/address_remap.h"

using namespace task_support;

namespace pimbridge {

// Forward declaration
class BankGroupCommand;

// Task classification information structure
struct TaskClassification {
    uint32_t localDataTasks;
    std::unordered_map<uint32_t, uint32_t> managedDataTasks;
    
    TaskClassification() : localDataTasks(0) {}
};

class CommModuleBase {
protected:
    std::string name;
    uint32_t level;
    uint32_t commId;

    uint32_t bankBeginId; 
    uint32_t bankEndId;
    uint32_t parentId;
    CommPacketQueue parentPackets;
    CommPacketQueue lbParentPackets;

    // same-level direct communication 
    bool enableInterflow;
    uint32_t siblingBeginId;
    uint32_t siblingEndId;
    std::vector<CommPacketQueue> siblingPackets; 

    lock_t commLock;

    AddressRemapTable* addrRemapTable;
    LoadBalancer* loadBalancer;

    // data collected from task unit during execution
    Counter s_GenTasks, s_FinishTasks;
    Counter s_GenPackets, s_RecvPackets;

    Counter s_ScheduleOutTasks, s_ScheduleInTasks, s_ScheduleOutData, s_ScheduleInData;
    // Counter s_InAndOutData, s_OutAndInData;

    double executeSpeed;

public:
    // initialization
    CommModuleBase(uint32_t _level, uint32_t _commId, 
        Config& config, const std::string& prefix);
    void initSiblings(uint32_t sibBegin, uint32_t sibEnd);

    virtual uint64_t communicate(uint64_t curCycle) = 0; 
    virtual uint64_t gather(uint64_t curCycle) { return curCycle; }
    virtual uint64_t scatter(uint64_t curCycle) { return curCycle; }
    virtual void gatherState() = 0;
    virtual void gatherTransferState() {}
    virtual bool isEmpty(uint64_t ts = 0); // pass ts = 0 means allEmpty
    void receivePackets(CommModuleBase* srcModule, 
                        uint32_t messageSize, uint64_t readyCycle, 
                        uint32_t& numPackets, uint32_t& totalSize);
    virtual CommPacket* nextPacket(uint32_t fromLevel, uint32_t fromCommId, 
                                   uint32_t sizeLimit) = 0;
    void handleOutPacket(CommPacket* packet);  

    // load balance
    virtual void commandLoadBalance(bool* needParentLevelLb) = 0;
    virtual void executeLoadBalance(const LbCommand& command,  
        uint32_t targetBankId, std::vector<DataHotness>& outInfo) = 0;
    
    // Bank Group负载均衡接口（默认实现为空，仅在CommModule中实现）
    virtual void executeBankGroupLoadBalance(const BankGroupCommand& command, uint32_t sourceBankId) {
        // 默认空实现，避免纯虚函数调用错误
    }
        
    virtual void addToSteal(uint64_t val) { panic("?!"); }
    virtual uint64_t getToSteal() { panic("?!"); }
    virtual void clearToSteal() { panic("?!"); }

    // state that accessed by filtered command
    uint64_t stateTransferRegionSize();
    
    // getters and setters
    void setParentId(uint32_t _parentId) { this->parentId = _parentId; }
    const char* getName() { return this->name.c_str(); }
    uint32_t getBankBeginId() { return bankBeginId; }
    uint32_t getBankEndId() { return bankEndId; }
    uint32_t getNumBanks() {return bankEndId - bankBeginId; }
    uint32_t getLevel() { return this->level; }
    uint32_t getCommId() { return this->commId; }
    LoadBalancer* getLoadBalancer() { return this->loadBalancer; }
    double getExecuteSpeed() { return this->executeSpeed; } // #task per cycle
    double getTransferSpeed() { return 0; } // TBY TODO: // #bytes per cycle
    AddressRemapTable* getRemapTable() { return this->addrRemapTable; }
    void setLoadBalancer(LoadBalancer* lb) { this->loadBalancer = lb; }

    void newAddrLend(Address lbPageAddr);
    void newAddrRemap(Address lbPageAddr, uint32_t dst, bool isMidState = false);

protected:
    virtual bool isChild(int id) = 0;
    virtual void handleInPacket(CommPacket* packet) = 0;
    void interflow(uint32_t sibId, uint32_t messageSize);
    bool isSibling(int id) {
        return (id >= 0 && (uint32_t)id >= siblingBeginId && 
            (uint32_t)id <= siblingEndId && (uint32_t)id != commId);
    }
    // >= 0: available location
    // -1: not available
    // -2: being transferred
public:
    virtual int checkAvailable(Address lbPageAddr) = 0;

public:
    virtual void initStats(AggregateStat* parentStat) {}
};

class PimBridgeTaskUnit;

class BottomCommModule : public CommModuleBase {
private:
    uint64_t toStealSize;

    std::unordered_map<Address, DataLendCommPacket*> toLendMap;
public:
    PimBridgeTaskUnit* taskUnit;
    BottomCommModule(uint32_t _level, uint32_t _commId, 
                     Config& config, const std::string& prefix, 
                     PimBridgeTaskUnit* _taskUnit);
    void gatherState() override;
    uint64_t communicate(uint64_t curCycle) override { return curCycle; }
    CommPacket* nextPacket(uint32_t fromLevel, uint32_t fromCommI, 
                           uint32_t sizeLimit) override;
    void commandLoadBalance(bool* needParentLevelLb) override { return; }
    void executeLoadBalance(const LbCommand& command, 
        uint32_t targetBankId, std::vector<DataHotness>& outInfo) override;

    void addToSteal(uint64_t val) override {
        this->toStealSize += val;
        // info("module %s addToSteal: %lu, total: %lu", this->getName(), val, toStealSize);
    }
    uint64_t getToSteal() override {
        return this->toStealSize;
    }
    void clearToSteal() override {
        this->toStealSize = 0;
    }
    void pushDataLendPackets();
private:
    void handleInPacket(CommPacket* packet) override;
    // 1 for available
public:
    int checkAvailable(Address lbPageAddr) override;
private:   
    bool isChild(int id) override {
        return ((uint32_t)id == this->commId);
    }
public:
    void initStats(AggregateStat* parentStat) override;

    friend class PimBridgeTaskUnitKernel;
    friend class PimBridgeTaskUnit;
    friend class ReserveLbPimBridgeTaskUnitKernel;
    friend class LimitedReserveLbPimBridgeTaskUnitKernel;
};

class CommModule : public CommModuleBase {
private:
    uint32_t childBeginId;
    uint32_t childEndId;
    GatherScheme* gatherScheme;
    ScatterScheme* scatterScheme;

    // state information
    uint64_t lastGatherPhase;
    uint64_t lastScatterPhase;

    // 负载均衡频率控制
    uint64_t loadBalancePhaseCounter;
    uint64_t loadBalancePhaseInterval;
    uint64_t lastLoadBalancePhase;

    // packet buffer
    std::vector<CommPacketQueue> scatterBuffer;

    std::vector<uint64_t> childTransferSize;

    std::vector<uint64_t> bankQueueLength;
    std::vector<uint64_t> bankQueueReadyLength;
    std::vector<uint64_t> bankTransferSize;

    bool enableLoadBalance;

public:
    CommModule(uint32_t _level, uint32_t _commId, 
               Config& config, const std::string& prefix, 
               uint32_t _childBeginId, uint32_t _childEndId, 
               GatherScheme* _gatherScheme, ScatterScheme* _scatterScheme, 
               bool _enableLoadBalance);
    
    uint64_t communicate(uint64_t curCycle) override;

    uint64_t gather(uint64_t curCycle) override;
    uint64_t scatter(uint64_t curCycle) override;

    CommPacket* nextPacket(uint32_t fromLevel, uint32_t fromCommId, 
                           uint32_t sizeLimit) override;
    void gatherState() override; 
    void gatherTransferState() override; 
    void commandLoadBalance(bool* needParentLevelLb) override;
    void executeLoadBalance(const LbCommand& command, 
        uint32_t targetBankId, std::vector<DataHotness>& outInfo);
    
    // Bank Group负载均衡新接口
    void executeBankGroupLoadBalance(const BankGroupCommand& command, uint32_t sourceBankId);
    void updateBankGroupMapping(const std::vector<uint32_t>& newGroupAssignments);
    void handleDataReassignment(Address addr, uint32_t newOwnerBank);
    std::vector<uint32_t> getBankQueueLengths();
    void updateBankTypes(const std::vector<bool>& activeFlags, const std::vector<bool>& storageFlags);
    
    // 任务分类接口
    TaskClassification getTaskClassification(uint32_t activeBankId);
    std::vector<Address> getStorageBankAddresses(uint32_t storageBankId);
    
    // 存储Bank重分配接口
    void executeStorageBankReassignment(const StorageBankReassignment& reassignment);
    
    // 地址重映射接口
    void executeAddressRemapping(const std::pair<uint64_t, uint64_t>& remapping);
    
    // 任务迁移接口
    void executeTaskMigration(const TaskMigration& migration);
        
    bool isEmpty(uint64_t ts = 0) override;
    
    // getters & setters
    uint64_t getLastGatherPhase() { return this->lastGatherPhase; }
    uint64_t getLastScatterPhase() { return this->lastScatterPhase; }

private: 
    void handleInPacket(CommPacket* packet) override;
    void handleToChildPacket(CommPacket* packet, uint32_t childCommId);
public:
    int checkAvailable(Address lbPageAddr) override;
private:
    bool isChild(int id) override {
        return (id >= 0 && (uint32_t)id >= this->bankBeginId && 
            (uint32_t)id < this->bankEndId);
    }
    bool shouldCommandLoadBalance();

public:
    void initStats(AggregateStat* parentStat) override;
private:
    Counter s_GatherTimes, s_ScatterTimes, s_GatherPackets, s_ScatterPackets;
    VectorCounter sv_GatherPackets, sv_ScatterPackets;

    friend class pimbridge::GatherScheme;
    friend class pimbridge::IntervalGather;
    friend class pimbridge::OnDemandGather;
    friend class pimbridge::OnDemandOfAllGather;
    friend class pimbridge::WheneverGather;
    friend class pimbridge::NeverGather;
    friend class pimbridge::DynamicGather;
    friend class pimbridge::DynamicIntervalGather;
    friend class pimbridge::TaskGenerationTrackGather;

    friend class pimbridge::AfterGatherScatter;
    friend class pimbridge::IntervalScatter;
    friend class pimbridge::OnDemandScatter;

    friend class pimbridge::LoadBalancer;
    friend class pimbridge::StealingLoadBalancer;
    friend class pimbridge::MultiVictimStealingLoadBalancer;
    friend class pimbridge::ReserveLoadBalancer;
    friend class pimbridge::TryReserveLoadBalancer;
    friend class pimbridge::FastArriveLoadBalancer;
};

    
} // namespace pimbridge
