#pragma once
#include <map>
#include <unordered_map>
#include <queue>
#include <deque>
#include "task_support/task_unit.h"
#include "task_support/pim_bridge_task_unit.h"
#include "load_balancing/reserve_sketch.h"
#include "load_balancer.h"


namespace pimbridge {

using namespace task_support;

class ReserveLbPimBridgeTaskUnitKernel 
    : public PimBridgeTaskUnitKernel {
public:
    MemSketch sketch;
protected:
    uint64_t reserveRegionSize;
    std::unordered_map<Address, 
        std::priority_queue<TaskPtr, std::deque<TaskPtr>, cmp>> reserveRegion;
public:
    ReserveLbPimBridgeTaskUnitKernel(uint32_t _tuId, uint32_t _kernelId, 
        uint32_t numBucket, uint32_t bucketSize);

    void taskEnqueueKernel(TaskPtr t, int available) override;
    TaskPtr taskDequeueKernel() override;
    bool isEmpty() override;
    uint64_t getReadyTaskQueueSize() override;
    uint64_t getAllTaskQueueSize() override;

    void executeLoadBalanceCommand(
        const LbCommand& command, 
        std::vector<DataHotness>& outInfo) override;
        
    void prepareLbState() override;

    void exitReserveState(Address lbPageAddr);
protected:
    bool shouldReserve(TaskPtr t);
    TaskPtr reservedTaskDequeue();
    void reservedTaskEnqueue(TaskPtr t);

    friend class ReserveLoadBalancer;
};

class LimitedReserveLbPimBridgeTaskUnitKernel 
    : public ReserveLbPimBridgeTaskUnitKernel {
public:
    LimitedReserveLbPimBridgeTaskUnitKernel(uint32_t _tuId, uint32_t _kernelId, 
        uint32_t numBucket, uint32_t bucketSize) 
        : ReserveLbPimBridgeTaskUnitKernel(_tuId, _kernelId, numBucket, bucketSize){}
    void executeLoadBalanceCommand(
        const LbCommand& command, 
        std::vector<DataHotness>& outInfo) override;
    friend class ReserveLoadBalancer;
};

}