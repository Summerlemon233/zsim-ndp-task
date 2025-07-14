#include <deque>
#include "stats.h"
#include "core.h"
#include "config.h"
#include "zsim.h"
#include "comm_support/comm_module.h"
#include "comm_support/comm_mapping.h"
#include "gather_scheme.h"
#include "scatter_scheme.h"
#include "numa_map.h"
#include "load_balancing/bank_group_load_balancer.h"

using namespace pimbridge;
using namespace task_support;


CommModule::CommModule(uint32_t _level, uint32_t _commId, 
                       Config& config, const std::string& prefix, 
                       uint32_t _childBeginId, uint32_t _childEndId, 
                       GatherScheme* _gatherScheme, 
                       ScatterScheme* _scatterScheme, 
                       bool _enableLoadBalance) 
    : CommModuleBase(_level, _commId, config, prefix), 
      childBeginId(_childBeginId), childEndId(_childEndId), 
      gatherScheme(_gatherScheme),scatterScheme(_scatterScheme), 
      lastGatherPhase(0), lastScatterPhase(0), 
      enableLoadBalance(_enableLoadBalance) {
    info("---build comm module: childBegin: %u, childEnd: %u", childBeginId, childEndId);
    assert(this->level > 0);
    this->bankBeginId = zinfo->commModules[level-1][childBeginId]->getBankBeginId();
    this->bankEndId = zinfo->commModules[level-1][childEndId-1]->getBankEndId();
    zinfo->commMapping->setMapping(level, bankBeginId, bankEndId, commId);
    info("begin Id: %u, endId: %u", bankBeginId, bankEndId);
    info("enable lb: %d", enableLoadBalance);
    this->scatterBuffer.resize(childEndId - childBeginId);
    gatherScheme->setCommModule(this);
    scatterScheme->setCommModule(this);

    this->bankQueueLength.resize(bankEndId - bankBeginId);
    this->bankQueueReadyLength.resize(bankEndId - bankBeginId);
    this->bankTransferSize.resize(bankEndId - bankBeginId);
    this->childTransferSize.resize(childEndId - childBeginId);
}

uint64_t CommModule::communicate(uint64_t curCycle) {
    uint64_t respCycle = curCycle;
    respCycle = gather(respCycle);
    respCycle = scatter(respCycle);
    /*
    // info("resp before gather: %lu", respCycle);
    if (this->gatherScheme->shouldTrigger()) {
        respCycle = gather(respCycle);
    }
    // info("resp after gather: %lu", respCycle);
    if (this->scatterScheme->shouldTrigger()) {
        respCycle = scatter(respCycle);
    }
    // info("resp after scatter: %lu", respCycle);
    */
    return respCycle;
}

CommPacket* CommModule::nextPacket(uint32_t fromLevel, uint32_t fromCommId, 
                                   uint32_t sizeLimit) {
    CommPacketQueue* cpd = nullptr;
    if (fromLevel == this->level - 1) {
        // scatter
        cpd = &(this->scatterBuffer[fromCommId - childBeginId]);
    } else if (fromLevel == this->level) {
        // interflow
        cpd = &(this->siblingPackets[fromCommId - siblingBeginId]);
    } else if (fromLevel == this->level + 1) {
        // gather
        cpd = &(this->parentPackets);
    } else {
        panic("invalid fromLevel %u for nextPacket from CommModule", fromLevel);
    }
    CommPacket* ret = cpd->front();
    if (ret != nullptr && ret->getSize() < sizeLimit) {
        cpd->pop();
        return ret;
    }
    return nullptr;
}

void CommModule::commandLoadBalance(bool* needParentLevelLb) {
    if (!this->shouldCommandLoadBalance()) {
        return;
    }
    DEBUG_LB_O("module %s begin command lb", this->getName());
    
    // 检查是否使用新的Bank Group负载均衡器
    BankGroupLoadBalancer* bankGroupLB = dynamic_cast<BankGroupLoadBalancer*>(this->loadBalancer);
    if (bankGroupLB != nullptr) {
        // 更新Bank负载信息
        std::vector<uint32_t> queueLengths = getBankQueueLengths();
        bankGroupLB->updateBankLoads(queueLengths);
        
        // 生成Bank Group重分配命令
        bankGroupLB->generateCommand(needParentLevelLb);
        
        // 执行Bank Group重分配
        std::vector<DataHotness> outInfo;
        for (uint32_t i = this->bankBeginId; i < bankEndId; ++i) {
            const BankGroupCommand& cmd = bankGroupLB->getBankGroupCommand(i-bankBeginId);
            if (!cmd.empty()) {
                uint32_t childCommId = zinfo->commMapping->getCommId(level-1, i);
                zinfo->commModules[level-1][childCommId]->executeBankGroupLoadBalance(cmd, i);
            }
        }
        
        bankGroupLB->assignLbTarget(outInfo);
    } else {
        // 使用传统的负载均衡机制
        this->loadBalancer->generateCommand(needParentLevelLb);
        std::vector<DataHotness> outInfo;
        outInfo.clear();
        for (uint32_t i = this->bankBeginId; i < bankEndId; ++i) {
            const LbCommand& curCommand = loadBalancer->commands[i-bankBeginId];
            uint32_t childCommId = zinfo->commMapping->getCommId(level-1, i);
            if (!curCommand.empty()) {
                zinfo->commModules[level-1][childCommId]->executeLoadBalance(curCommand, i, outInfo);
            }
        }
        this->loadBalancer->assignLbTarget(outInfo);
    }   
}

void CommModule::executeLoadBalance(
        const LbCommand& command, uint32_t targetBankId, 
        std::vector<DataHotness>& outInfo) {
    DEBUG_LB_O("comm %s execute lb", this->getName());
    uint64_t curOutSize = outInfo.size();
    uint32_t childCommId = zinfo->commMapping->getCommId(level-1, targetBankId);
    zinfo->commModules[level-1][childCommId]
        ->executeLoadBalance(command, targetBankId, outInfo);
    for (uint64_t i = curOutSize; i < outInfo.size(); ++i) {
        this->newAddrLend(outInfo[i].addr);
    }
    DEBUG_LB_O("comm %s end execute lb", this->getName());
}

bool CommModule::isEmpty(uint64_t ts) {
    if (!CommModuleBase::isEmpty(ts)) {
        return false;
    }
    for (auto pq : this->scatterBuffer) {
        if (!pq.empty(ts)) { return false; }
    }
    return true;
}

void CommModule::handleInPacket(CommPacket* packet) {
    assert(packet->toLevel == this->level);
    s_RecvPackets.atomicInc(1);
    int avail = this->checkAvailable(packet->getAddr());
    if (avail == -1) {
        this->handleOutPacket(packet);
    } else {
        assert(avail >= 0);
        uint32_t availLoc = (uint32_t)avail;
        if (availLoc == packet->fromCommId) {
            // info("comm %s back to from %u packet: type: %u, addr: %lu", 
            //     this->getName(), availLoc, packet->getInnerType(), packet->getAddr());
            assert(zinfo->commModules[level-1][availLoc]->checkAvailable(packet->getAddr()) != -1);
        }
        this->handleToChildPacket(packet, availLoc);
    }
}

void CommModule::handleToChildPacket(CommPacket* packet, uint32_t childCommId) {
    packet->fromLevel = this->level;
    packet->fromCommId = this->commId;
    packet->toLevel = this->level - 1;
    packet->toCommId = childCommId;
    this->scatterBuffer[childCommId - childBeginId].push(packet);
}

int CommModule::checkAvailable(Address lbPageAddr) {
    Address pageAddr = zinfo->numaMap->getPageAddressFromLbPageAddress(lbPageAddr);
    uint32_t nodeId = zinfo->numaMap->getNodeOfPage(pageAddr);
    int remap = this->addrRemapTable->getChildRemap(lbPageAddr);
    if (remap != -1) {
        assert(!this->addrRemapTable->getAddrLend(lbPageAddr));
        return remap;
    } else {
        assert(!this->addrRemapTable->getAddrLend(lbPageAddr) || isChild(nodeId));
        if (isChild(nodeId) && !this->addrRemapTable->getAddrLend(lbPageAddr)) {
            return zinfo->commMapping->getCommId(this->level-1, nodeId);
        } else {
            return -1;
        }
    }
}

uint64_t CommModule::gather(uint64_t curCycle) {
    // info("gather: %u-%u", level, commId);
    if (!gatherScheme->shouldTrigger()) {
        return curCycle;
    }
    uint64_t readyCycle = curCycle;
    if (this->level == 1) {
        for (uint32_t i = childBeginId; i < childEndId; ++i) {
            uint64_t respCycle = zinfo->cores[i]->recvCommReq(true, curCycle, i, 
                (this->gatherScheme->packetSize - 64));
                // 8);
            // info("resp of %u: %lu", i, respCycle);
            readyCycle = respCycle > readyCycle ? respCycle : readyCycle;
        }
    }

    zinfo->gatherProfiler->initTransfer(this->level, this->commId);

    for (size_t i = childBeginId; i < childEndId; ++i) {
        CommModuleBase* src = zinfo->commModules[this->level-1][i];
        uint32_t numPackets = 0, totalSize = 0;
        uint32_t packetSize = this->gatherScheme->packetSize;
        
        this->receivePackets(src, packetSize, readyCycle, numPackets, totalSize);
        this->sv_GatherPackets.atomicInc(i-childBeginId, numPackets);
        this->s_GatherPackets.atomicInc(numPackets);
        zinfo->gatherProfiler->record(this->level, this->commId, 
            i-childBeginId, totalSize);
    }

    this->lastGatherPhase = zinfo->numPhases;
    this->s_GatherTimes.atomicInc(1);
    return readyCycle;
}

uint64_t CommModule::scatter(uint64_t curCycle) {
    if (!scatterScheme->shouldTrigger()) {
        return curCycle;
    }
    uint64_t readyCycle = curCycle;
    if (this->level == 1) {
        for (uint32_t i = childBeginId; i < childEndId; ++i) {
            uint64_t respCycle = zinfo->cores[i]->recvCommReq(false, curCycle, 
                i, this->scatterScheme->packetSize);
            readyCycle = respCycle > readyCycle ? respCycle : readyCycle;
        }
    }
    for (size_t i = childBeginId; i < childEndId; ++i) {
        uint32_t numPackets = 0, totalSize = 0;
        zinfo->commModules[level-1][i]->
            receivePackets(this, this->scatterScheme->packetSize, readyCycle, 
                           numPackets, totalSize);
        this->sv_ScatterPackets.atomicInc(i-childBeginId, numPackets);
        this->s_GatherPackets.atomicInc(numPackets);
    }
    this->s_ScatterTimes.atomicInc(1);
    this->lastScatterPhase = zinfo->numPhases;
    return readyCycle;
}

void CommModule::gatherState() {
    DEBUG_GATHER_STATE_O("module %s gather state", this->getName());
    for (uint32_t i = bankBeginId; i < bankEndId; ++i) {
        uint32_t id = i - bankBeginId;
        this->bankQueueLength[id] = 
            zinfo->taskUnits[i]->getCurUnit()->getAllTaskQueueSize();
        this->bankQueueReadyLength[id] = 
            zinfo->taskUnits[i]->getCurUnit()->getReadyTaskQueueSize();
        this->bankTransferSize[id] = 
            zinfo->commModules[0][i]->stateTransferRegionSize();
        if (this->level == zinfo->commModules.size()-1) {
            if (bankQueueLength[id] != 0) {
                DEBUG_GATHER_STATE_O("bank %u queueLength %lu readyLength %lu", 
                    i, bankQueueLength[id],bankQueueReadyLength[id])
            }
        }
    }
    this->executeSpeed = 0;
    for (uint32_t i = childBeginId; i < childEndId; ++i) {
        CommModuleBase* child = zinfo->commModules[level-1][i];
        this->executeSpeed += child->getExecuteSpeed();
        this->childTransferSize[i - childBeginId] = child->stateTransferRegionSize();
        if (this->childTransferSize[i - childBeginId]!=0) {
            DEBUG_GATHER_STATE_O("child %s transferLength %lu", 
                child->getName(), childTransferSize[i - childBeginId]);
        }
    }
}

void CommModule::gatherTransferState() {
    DEBUG_GATHER_STATE_O("module %s gather transfer state", this->getName());
    for (uint32_t i = bankBeginId; i < bankEndId; ++i) {
        this->bankTransferSize[i - bankBeginId] = 
            zinfo->commModules[0][i]->stateTransferRegionSize();
    }
    for (uint32_t i = childBeginId; i < childEndId; ++i) {
        CommModuleBase* child = zinfo->commModules[level-1][i];
        this->childTransferSize[i - childBeginId] = child->stateTransferRegionSize();
    }
}

bool CommModule::shouldCommandLoadBalance() {
    // TBY TODO: delete this function. add to commandLoadBalance directly
    if (!this->enableLoadBalance) {
        return false;
    }
    return true;
}

void CommModule::initStats(AggregateStat* parentStat) {
    AggregateStat* commStat = new AggregateStat();
    commStat->init(name.c_str(), "Communication module stats");

    s_GenTasks.init("genTasks", "Number of generated tasks");
    commStat->append(&s_GenTasks);
    s_FinishTasks.init("finishTasks", "Number of finished tasks");
    commStat->append(&s_FinishTasks);

    s_GenPackets.init("genPackets", "Number of generated packets");
    commStat->append(&s_GenPackets);
    s_RecvPackets.init("recvPackets", "Number of received packets");
    commStat->append(&s_RecvPackets);

    s_GatherTimes.init("gatherTimes", "Number of gathering");
    commStat->append(&s_GatherTimes);
    s_GatherPackets.init("gatherPackets", "Number of gathered packets");
    commStat->append(&s_GatherPackets);
    s_ScatterTimes.init("scatterTimes", "Number of scattering");
    commStat->append(&s_ScatterTimes);
    s_ScatterPackets.init("scatterPackets", "Number of scattered packets");
    commStat->append(&s_ScatterPackets);

    s_ScheduleOutData.init("scheduleOutData", "Number of scheduled out data");
    commStat->append(&s_ScheduleOutData);
    s_ScheduleInData.init("scheduleInData", "Number of scheduled in data");
    commStat->append(&s_ScheduleInData);
    s_ScheduleOutTasks.init("scheduleOutTasks", "Number of scheduled out tasks");
    commStat->append(&s_ScheduleOutTasks);
    s_ScheduleInTasks.init("scheduleInTasks", "Number of scheduled in tasks");
    commStat->append(&s_ScheduleInTasks);

    uint32_t numChild = childEndId - childBeginId;
    sv_GatherPackets.init("gatherPacketsPerChild", "Number of gathered packets per child", numChild);
    commStat->append(&sv_GatherPackets);
    sv_ScatterPackets.init("scatterPacketsPerChild", "Number of scattered packets per child", numChild);
    commStat->append(&sv_ScatterPackets);

    parentStat->append(commStat);
}

// 新增的Bank Group负载均衡接口实现
void CommModule::executeBankGroupLoadBalance(const BankGroupCommand& command, uint32_t sourceBankId) {
    DEBUG_LB_O("comm %s execute bank group lb for bank %u", this->getName(), sourceBankId);
    
    // 处理数据重分配命令
    const auto& dataReassignments = command.getDataReassignments();
    for (const auto& reassignment : dataReassignments) {
        Address addr = reassignment.first;
        uint32_t newOwnerBank = reassignment.second;
        handleDataReassignment(addr, newOwnerBank);
    }
    
    // 处理Bank Group重分配命令
    const auto& groupReassignments = command.getGroupReassignments();
    if (!groupReassignments.empty()) {
        updateBankGroupMapping(groupReassignments);
    }
    
    DEBUG_LB_O("Bank group load balance executed: %zu data reassignments, %zu group reassignments",
              dataReassignments.size(), groupReassignments.size());
}

void CommModule::updateBankGroupMapping(const std::vector<uint32_t>& newGroupAssignments) {
    // 更新Bank到Group的映射关系
    // 这里需要与底层的地址重映射表协同工作
    for (size_t bankId = 0; bankId < newGroupAssignments.size(); bankId++) {
        uint32_t newGroupId = newGroupAssignments[bankId];
        if (newGroupId != UINT32_MAX) {
            DEBUG_LB_O("Bank %zu reassigned to group %u", bankId, newGroupId);
            // 这里可以添加具体的Group重分配逻辑
        }
    }
}

void CommModule::handleDataReassignment(Address addr, uint32_t newOwnerBank) {
    // 处理数据的重分配：更新地址重映射表
    if (this->addrRemapTable) {
        // 建立新的地址映射关系
        this->addrRemapTable->setChildRemap(addr, newOwnerBank);
        DEBUG_LB_O("Data at address 0x%lx reassigned to bank %u", addr, newOwnerBank);
    }
}

std::vector<uint32_t> CommModule::getBankQueueLengths() {
    std::vector<uint32_t> queueLengths;
    queueLengths.reserve(bankEndId - bankBeginId);
    
    for (uint32_t i = bankBeginId; i < bankEndId; i++) {
        queueLengths.push_back(static_cast<uint32_t>(bankQueueLength[i - bankBeginId]));
    }
    
    return queueLengths;
}

void CommModule::updateBankTypes(const std::vector<bool>& activeFlags, const std::vector<bool>& storageFlags) {
    // 更新Bank类型信息，用于负载均衡器
    BankGroupLoadBalancer* bankGroupLB = dynamic_cast<BankGroupLoadBalancer*>(this->loadBalancer);
    if (bankGroupLB != nullptr) {
        bankGroupLB->initializeBankTypes(activeFlags, storageFlags);
    }
}