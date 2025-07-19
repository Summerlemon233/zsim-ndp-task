#include "numa_map.h"
#include "stats.h"
#include "process_local_val.h"
#include "zsim.h"
#include "task_support/hint.h"
#include "task_support/task_unit.h"
#include "task_support/pim_bridge_task_unit.h"
#include "load_balancing/reserve_lb_task_unit.h"
#include "config.h"
#include "core.h"

using namespace task_support;
using namespace pimbridge;

void PimBridgeTaskUnitKernel::taskEnqueueKernel(TaskPtr t, int available) {
    // assert((this->curTs == 0 && t->timeStamp == 1 && this->kernelId == 1001) 
    //     || t->timeStamp == this->curTs);
    assert(available != -1);
    if (available == -2) {
        newNotReadyTask(t);
        return;
    }
    this->taskQueue.push(t);
}

// TBY TODO: we need to set the current cycle to available cycle
TaskPtr PimBridgeTaskUnitKernel::taskDequeueKernel() {
    TaskPtr ret = nullptr;
    if (this->taskQueue.empty()) {
        return this->endTask;
    } else {
        ret = this->taskQueue.top();
        this->taskQueue.pop();
    }
    assert_msg(ret->timeStamp == this->curTs, 
        "%u-%u, task ts: %lu, curTs: %lu, taskId: %lu", 
        taskUnitId, kernelId, ret->timeStamp, curTs, ret->taskId);
    int available = commModule->checkAvailable(
        zinfo->numaMap->getLbPageAddress(ret->hint->dataPtr));
    if (available >= 0) {
        return ret;
    } else if (available == -1) {
        uint64_t curCycle = zinfo->cores[taskUnitId]->getCurCycle();
        TaskCommPacket* p = new TaskCommPacket(ret->timeStamp, curCycle, 0, this->taskUnitId, 1, -1, ret);
        this->commModule->handleOutPacket(p);
        return taskDequeueKernel();
    } else if (available == -2) {
        // This happens when a unit lends a data and then borrows it back
        newNotReadyTask(ret);
        return taskDequeueKernel();
    } else {
        panic("invalid avail! %d", available);
    }
}

bool PimBridgeTaskUnitKernel::isEmpty() {
    return this->taskQueue.empty() && this->notReadyLbTasks.empty();
}

uint64_t PimBridgeTaskUnitKernel::getReadyTaskQueueSize(){
    return this->taskQueue.size();
}

uint64_t PimBridgeTaskUnitKernel::getAllTaskQueueSize(){
    return this->taskQueue.size() + this->notReadyTaskNumber
        + commModule->toStealSize;
}

void PimBridgeTaskUnitKernel::executeLoadBalanceCommand(
        const LbCommand& command,  
        std::vector<DataHotness>& outInfo) {
    uint64_t curCycle = zinfo->cores[taskUnitId]->getCurCycle();
    std::unordered_map<Address, uint32_t> info;
    for (auto curCommand : command.get()) {
        while (curCommand > 0 && !this->taskQueue.empty()) {
            TaskPtr t = this->taskQueue.top();
            assert(t->timeStamp == this->curTs);
            this->taskQueue.pop();
            Address lbPageAddr = zinfo->numaMap->getLbPageAddress(t->hint->dataPtr);
            int available = this->commModule->checkAvailable(lbPageAddr);
            if (available == -2) {
                newNotReadyTask(t);
            } else if (available == -1) {
                TaskCommPacket* p = new TaskCommPacket(t->timeStamp, curCycle, 0, this->taskUnitId, 1, -1, t, 3);
                this->commModule->handleOutPacket(p);
                --curCommand;
                this->commModule->s_ScheduleOutTasks.atomicInc(1);
                zinfo->commModuleManager->numSchedTasks.atomicInc(1);
                    zinfo->commModuleManager->schedTransferSize.atomicInc(t->taskSize);
            } else if (available >= 0) {
                TaskCommPacket* p = new TaskCommPacket(t->timeStamp, curCycle, 0, this->taskUnitId, 1, -1, t, 2);
                // DEBUG_LB_O("unit %u sched task out: addr: %lu, sig: %lu", taskUnitId, p->getAddr(), p->getSignature());
                this->commModule->handleOutPacket(p);
                if (info.count(lbPageAddr) == 0) {
                    info.insert(std::make_pair(lbPageAddr, 0));
                }
                info[lbPageAddr] += 1;
                --curCommand;
                this->commModule->s_ScheduleOutTasks.atomicInc(1);
                zinfo->commModuleManager->numSchedTasks.atomicInc(1);
                zinfo->commModuleManager->schedTransferSize.atomicInc(t->taskSize);
            } else {
                panic("invalid available value");
            }
        }
    }
    for (auto it = info.begin(); it != info.end(); ++it) {
        Address addr = it->first;
        DEBUG_LB_O("unit %u execute lb: addr: %lu, cnt: %u", taskUnitId, addr, it->second);
        outInfo.push_back(DataHotness(addr, this->taskUnitId, it->second));
        this->commModule->newAddrLend(addr);
        DataLendCommPacket* p = new DataLendCommPacket(this->curTs, curCycle, 0, this->taskUnitId,
            1, -1, addr, zinfo->lbPageSize);
        zinfo->commModuleManager->schedTransferSize.atomicInc(zinfo->lbPageSize);
        if (this->commModule->toLendMap.count(addr) == 0) {
            this->commModule->toLendMap.insert(std::make_pair(addr, p));
        }
    }
    if (!zinfo->taskUnits[taskUnitId]->getHasBeenVictim()) {
        zinfo->taskUnits[taskUnitId]->setHasBeenVictim(true);
    }
}

void PimBridgeTaskUnitKernel::newAddrBorrowKernel(Address lbPageAddr) {
    if (notReadyLbTasks.count(lbPageAddr) == 0) {
        return;
    }
    assert(commModule->checkAvailable(lbPageAddr) >= 0);
    std::deque<TaskPtr>& dq = notReadyLbTasks[lbPageAddr];
    while(!dq.empty()) {
        TaskPtr t = dq.front();
        dq.pop_front();
        this->taskEnqueueKernel(t, 0);
        assert(this->notReadyTaskNumber >= 1);
        --this->notReadyTaskNumber;
    }
    notReadyLbTasks.erase(lbPageAddr);
}

void PimBridgeTaskUnitKernel::newAddrReturnKernel(Address lbPageAddr) {
    if (notReadyLbTasks.count(lbPageAddr) == 0) {
        return;
    }
    std::deque<TaskPtr>& dq = notReadyLbTasks[lbPageAddr];
    uint64_t curCycle = zinfo->cores[taskUnitId]->getCurCycle();
    while(!dq.empty()) {
        TaskPtr t = dq.front();
        dq.pop_front();
        TaskCommPacket* p = new TaskCommPacket(t->timeStamp, curCycle, 0, this->taskUnitId, 1, -1, t, 3);
        this->commModule->handleOutPacket(p);
        --this->notReadyTaskNumber;
    }
    notReadyLbTasks.erase(lbPageAddr);
}

void PimBridgeTaskUnitKernel::newNotReadyTask(TaskPtr t) {
    Address lbPageAddr = zinfo->numaMap->getLbPageAddress(t->hint->dataPtr);
    if (this->notReadyLbTasks.find(lbPageAddr) == notReadyLbTasks.end()) {
        notReadyLbTasks.insert(std::make_pair(lbPageAddr, std::deque<TaskPtr>()));
    }
    this->notReadyLbTasks[lbPageAddr].push_back(t);
    ++this->notReadyTaskNumber;
}

PimBridgeTaskUnit::PimBridgeTaskUnit(const std::string& _name, uint32_t _tuId, 
                                     TaskUnitManager* _tum, Config& config) 
    : TaskUnit(_name, _tuId, _tum) {
    std::string taskUnitType = 
        config.get<const char*>("sys.taskSupport.taskUnitType");
    if (taskUnitType == "PimBridge") {
        this->taskUnit1 = new PimBridgeTaskUnitKernel(_tuId, 1001);
        this->taskUnit2 = new PimBridgeTaskUnitKernel(_tuId, 1002);
    } else if (taskUnitType == "ReserveLbPimBridge") {
        uint32_t numBucket = config.get<uint32_t>("sys.taskSupport.sketchBucketNum");
        uint32_t bucketSize = config.get<uint32_t>("sys.taskSupport.sketchBucketSize");
        this->taskUnit1 = new ReserveLbPimBridgeTaskUnitKernel(_tuId, 1001, numBucket, bucketSize);
        this->taskUnit2 = new ReserveLbPimBridgeTaskUnitKernel(_tuId, 1002, numBucket, bucketSize);
    } else if (taskUnitType == "LimitedReserveLbPimBridge") {
        uint32_t numBucket = config.get<uint32_t>("sys.taskSupport.sketchBucketNum");
        uint32_t bucketSize = config.get<uint32_t>("sys.taskSupport.sketchBucketSize");
        this->taskUnit1 = new LimitedReserveLbPimBridgeTaskUnitKernel(_tuId, 1001, numBucket, bucketSize);
        this->taskUnit2 = new LimitedReserveLbPimBridgeTaskUnitKernel(_tuId, 1002, numBucket, bucketSize);
    }
    this->useQ1 = false;
    this->curTaskUnit = this->taskUnit2;
    this->nxtTaskUnit = this->taskUnit1;  
}


void PimBridgeTaskUnit::assignNewTask(TaskPtr t, Hint* hint) {
    //info("Assigning new Tasks");
    assert(hint->dataPtr != 0);
    
    uint32_t nodeId;
    
    // Unified logic for both first-round and non-first-round tasks
    if (hint->location >= 0) {
        // Explicit location specified - validate and potentially auto-route
        assert_msg((uint32_t)hint->location < zinfo->numCores, 
            "Invalid NUMA node ID %d specified in hint. Must be less than numCores (%u)", 
            hint->location, zinfo->numCores);
        
        if (zinfo->TASK_BASED && zinfo->enableComputeRelocation) {
            if (zinfo->isActiveNode[hint->location]) {
                // Specified location is an active node, use it directly
                nodeId = hint->location;
            } else {
                // Specified location is a storage node, auto-route to responsible active node
                nodeId = zinfo->storageToActiveMap[hint->location];
                info("Auto-routing: task specified for storage node %d, routed to responsible active node %d", 
                     hint->location, nodeId);
            }
        } else {
            // Fallback: use specified location as-is if compute relocation is disabled
            nodeId = hint->location;
        }
    } else {
        // No explicit location (hint->location == -1) - use computation relocation strategy
        uint32_t dataNodeId = zinfo->numaMap->getNodeOfPage(zinfo->numaMap->getPageAddress(hint->dataPtr));
        
        if (zinfo->TASK_BASED && zinfo->enableComputeRelocation) {
            if (zinfo->isActiveNode[dataNodeId]) {
                // Data is on an active node, execute there
                nodeId = dataNodeId;
            } else {
                // Data is on a storage node, route to responsible active node
                nodeId = zinfo->storageToActiveMap[dataNodeId];
                info("Computation relocation: data at storage node %d, task routed to active node %d", 
                     dataNodeId, nodeId);
            }
        } else {
            // Fallback to original behavior if compute relocation is disabled
            nodeId = dataNodeId;
        }
    }
    
    // Final validation: ensure target node is active (if compute relocation is enabled)
    if (zinfo->TASK_BASED && zinfo->enableComputeRelocation) {
        assert_msg(zinfo->isActiveNode[nodeId], 
            "Task must be executed on active node. Node %d is not active.", nodeId);
    }
    
    // Enqueue task to the determined node
    if (hint->firstRound) {
        assert(t->timeStamp == 1);
        // info("First round: Task enqueued to node %d", nodeId);
    } else {
        this->commModule->s_GenTasks.atomicInc(1);
        // info("Non-first round: Task enqueued to node %d", nodeId);
    }
    
    zinfo->taskUnits[nodeId]->taskEnqueue(t, 0);
}

void PimBridgeTaskUnit::newAddrBorrow(Address lbPageAddr) {
    ((PimBridgeTaskUnitKernel*)this->curTaskUnit)->newAddrBorrowKernel(lbPageAddr);
    ((PimBridgeTaskUnitKernel*)this->nxtTaskUnit)->newAddrBorrowKernel(lbPageAddr);
}

void PimBridgeTaskUnit::newAddrReturn(Address lbPageAddr) {
    ((PimBridgeTaskUnitKernel*)this->curTaskUnit)->newAddrReturnKernel(lbPageAddr);
    ((PimBridgeTaskUnitKernel*)this->nxtTaskUnit)->newAddrReturnKernel(lbPageAddr);
}

void PimBridgeTaskUnit::setCommModule(BottomCommModule* _commModule) { 
    this->commModule = _commModule; 
    ((PimBridgeTaskUnitKernel*)taskUnit1)->commModule = _commModule;
    ((PimBridgeTaskUnitKernel*)taskUnit2)->commModule = _commModule;
}
