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
    TaskPtr endTask; // when calling taskDequeue when the unit is empty, endTask will be returned
public:
    TaskUnitKernel(uint32_t _tuId) : taskUnitId(_tuId) {}
    virtual ~TaskUnitKernel() {}
    
    virtual void taskEnqueueKernel(TaskPtr t, int available) = 0;
    virtual TaskPtr taskDequeueKernel() = 0;
    virtual bool isEmpty() = 0;
    virtual uint64_t getReadyTaskQueueSize() = 0;
    virtual uint64_t getAllTaskQueueSize() = 0;
    virtual void executeLoadBalanceCommand(
        const pimbridge::LbCommand& command, 
        std::vector<pimbridge::DataHotness>& outInfo) {}

    // for ReserveBased;
    virtual uint64_t getTopItemLength() { return 0; }
    virtual void prepareState() {}

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

    double executeSpeed;

public:
    TaskUnit(const std::string& _name, uint32_t _tuId, TaskUnitManager* _tum);
    virtual ~TaskUnit();

    virtual void assignNewTask(TaskPtr t, Hint* hint) = 0;
    void taskEnqueue(TaskPtr t, int available);
    TaskPtr taskDequeue();
    void taskFinish(TaskPtr t);
    void beginRun(uint64_t newTs);

    // Getters and setters
    TaskUnitKernel* getCurUnit() { return curTaskUnit; }
    void setEndTask(TaskPtr t);
    TaskPtr getEndTask() { return this->endTask; }
    const char* getName() { return name.c_str(); }
    uint32_t getTaskUnitId() { return this->taskUnitId; }
    uint64_t getMinTimeStamp() { return this->minTimeStamp; }

    void initStats(AggregateStat* parentStat);

    void computeExecuteSpeed();

protected:
    void switchUnit();
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
    TaskUnitManager() : finishUnitNumber(0) {
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