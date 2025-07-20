#pragma once
#include "task_support/task_unit.h"
#include "config.h"
 
namespace pimbridge {

using namespace task_support;
struct cmp {
    bool operator()(const TaskPtr& t1, const TaskPtr& t2) const {
        if (t1->readyCycle != t2->readyCycle) {
            return t1->readyCycle > t2->readyCycle;
        } else {
            return t1->taskId > t2->taskId;
        }
    }
};

class PimBridgeTaskUnitKernel : public TaskUnitKernel {
protected:
    uint32_t notReadyTaskNumber;
    BottomCommModule* commModule;   
    std::unordered_map<Address, std::deque<TaskPtr>> notReadyLbTasks;
    std::priority_queue<TaskPtr, std::deque<TaskPtr>, cmp> taskQueue;
public:
    PimBridgeTaskUnitKernel(uint32_t _tuId, uint32_t _kernelId) 
        : TaskUnitKernel(_tuId, _kernelId), notReadyTaskNumber(0) {}
    void taskEnqueueKernel(TaskPtr t, int available) override;
    TaskPtr taskDequeueKernel() override;
    bool isEmpty() override;
    uint64_t getReadyTaskQueueSize() override;
    uint64_t getAllTaskQueueSize() override;

    void executeLoadBalanceCommand(
        const LbCommand& command, 
        std::vector<DataHotness>& outInfo) override;
    
    // Bank Group load balancing task migration interfaces
    std::vector<TaskPtr> extractTasksForStorageBank(uint32_t storageBankId, uint32_t maxTasks) override;
    void injectTasksFromMigration(const std::vector<TaskPtr>& tasks) override;
    uint32_t countTasksForStorageBank(uint32_t storageBankId) override;
    
    void setCommModule(BottomCommModule* _commModule) {
        this->commModule = _commModule;
    }

protected:
    void newNotReadyTask(TaskPtr t);
    void newAddrBorrowKernel(Address lbPageAddr);
    void newAddrReturnKernel(Address lbPageAddr);

    friend class pimbridge::PimBridgeTaskUnit;
    friend class pimbridge::BottomCommModule;
};

class PimBridgeTaskUnit : public TaskUnit {
protected:
    BottomCommModule* commModule;
public:
    PimBridgeTaskUnit(const std::string& _name, uint32_t _tuId, 
                      TaskUnitManager* _tum, Config& config);
    void assignNewTask(TaskPtr t, Hint* hint) override;
    void setCommModule(BottomCommModule* _commModule);

    void newAddrBorrow(Address lbPageAddr);
    void newAddrReturn(Address lbPageAddr);
protected:
    friend class pimbridge::BottomCommModule;
};


} // namespace pimbridges