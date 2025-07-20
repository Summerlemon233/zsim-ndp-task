#pragma once
#include <deque>
#include <queue>
#include "galloc.h"
#include "g_std/g_vector.h"
#include "locks.h"
#include "stats.h"
#include "task_support/hint.h"
#include "task_support/task.h"
#include "comm_support/comm_packet.h"
#include "comm_support/comm_module.h"
#include "load_balancing/load_balancer.h"


namespace task_support {

class TaskUnitManager;

class TaskUnitKernel {
protected:
    const uint32_t taskUnitId; 
    const uint32_t kernelId;
    TaskPtr endTask; // when calling taskDequeue when the unit is empty, endTask will be returned
    uint64_t curTs;
public:
    TaskUnitKernel(uint32_t _tuId, uint32_t _kernelId) 
        : taskUnitId(_tuId),  kernelId(_kernelId), curTs(0) {}
    virtual ~TaskUnitKernel() {}
    
    virtual void taskEnqueueKernel(TaskPtr t, int available) = 0;
    virtual TaskPtr taskDequeueKernel() = 0;
    virtual bool isEmpty() = 0;
    virtual uint64_t getReadyTaskQueueSize() = 0;
    virtual uint64_t getAllTaskQueueSize() = 0;
    virtual void executeLoadBalanceCommand(
        const pimbridge::LbCommand& command, 
        std::vector<pimbridge::DataHotness>& outInfo) {}

    // Bank Group load balancing task migration interfaces
    virtual std::vector<TaskPtr> extractTasksForStorageBank(uint32_t storageBankId, uint32_t maxTasks) { return std::vector<TaskPtr>(); }
    virtual void injectTasksFromMigration(const std::vector<TaskPtr>& tasks) {}
    virtual uint32_t countTasksForStorageBank(uint32_t storageBankId) { return 0; }

    // for ReserveBased;
    virtual void prepareLbState() {}

    void setCurTs(uint64_t ts) { this->curTs = ts; }

protected:
    // Helper function to determine if a task belongs to a specific storage bank
    bool isTaskForStorageBank(TaskPtr task, uint32_t storageBankId);

    friend class TaskUnit;
};

class TaskUnit : public GlobAlloc {
protected:
    std::string name;
    const uint32_t taskUnitId; 
    TaskUnitManager* tum;
    lock_t tuLock;
    TaskPtr endTask; // when calling taskDequeue when the unit is empty, endTask will be returned
    bool isFinished;

    uint64_t minTimeStamp;
    bool useQ1;
    TaskUnitKernel* taskUnit1;
    TaskUnitKernel* taskUnit2;
    TaskUnitKernel* curTaskUnit;
    TaskUnitKernel* nxtTaskUnit;

    bool hasBeenVictim;
    bool hasReceiveLbTask;
    double executeSpeed;

public:
    TaskUnit(const std::string& _name, uint32_t _tuId, TaskUnitManager* _tum);
    virtual ~TaskUnit();

    virtual void assignNewTask(TaskPtr t, Hint* hint) = 0;
    void taskEnqueue(TaskPtr t, int available);
    TaskPtr taskDequeue();
    void taskFinish(TaskPtr t);
    void beginNewTimeStamp(uint64_t newTs);

    // Getters and setters
    TaskUnitKernel* getCurUnit() { return curTaskUnit; }
    void setEndTask(TaskPtr t);
    TaskPtr getEndTask() { return this->endTask; }
    const char* getName() { return name.c_str(); }
    uint32_t getTaskUnitId() { return this->taskUnitId; }
    uint64_t getMinTimeStamp() { return this->minTimeStamp; }
    bool getHasBeenVictim() { return hasBeenVictim; }
    void setHasBeenVictim(bool val) { this->hasBeenVictim = val; }
    bool getHasReceiveLbTask() { return hasReceiveLbTask; }
    void setHasReceiveLbTask(bool val) { this->hasReceiveLbTask = val; }

    void initStats(AggregateStat* parentStat);

    void computeExecuteSpeed();
    double getExecuteSpeed() {return this->executeSpeed;}

protected:
    void checkTimeStampChange(uint64_t newTs);

    Counter s_EnqueueTasks, s_DequeueTasks, s_FinishTasks;

};

class TaskUnitManager : public GlobAlloc {
protected:
    g_vector<TaskUnit*> taskUnits;
    lock_t tumLock;
    uint32_t finishUnitNumber;

    bool readyForNewTimeStamp;
    uint64_t allowedTimeStamp;

public:
    TaskUnitManager() : finishUnitNumber(0),readyForNewTimeStamp(true),allowedTimeStamp(0) {
        futex_init(&tumLock);
    }
    ~TaskUnitManager() {}

    void addTaskUnit(TaskUnit* tu) { this->taskUnits.push_back(tu); }

    void reportFinish(uint32_t tuId);
    void reportRestart();
    void reportChangeAllowedTimestamp(uint32_t taskUnitId);

    bool allFinish();
    void finishTimeStamp();
    void beginRun();
    uint64_t getAllowedTimeStamp() { return allowedTimeStamp; }
    bool getReadyForNewTimeStamp() { return readyForNewTimeStamp; }
};


}