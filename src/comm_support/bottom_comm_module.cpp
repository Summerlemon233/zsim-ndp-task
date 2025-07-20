#include <deque>
#include "stats.h"
#include "core.h"
#include "zsim.h"
#include "numa_map.h"
#include "config.h"
#include "comm_support/comm_module.h"
#include "comm_support/comm_mapping.h"
#include "gather_scheme.h"
#include "scatter_scheme.h"
#include "task_support/pim_bridge_task_unit.h"

using namespace pimbridge;
using namespace task_support;

BottomCommModule::BottomCommModule(uint32_t _level, uint32_t _commId, 
                                   Config& config, const std::string& prefix, 
                                   PimBridgeTaskUnit* _taskUnit)
    : CommModuleBase(_level, _commId, config, prefix), taskUnit(_taskUnit) {
    bankBeginId = commId;
    bankEndId = commId + 1;
    info("begin Id: %u, endId: %u", bankBeginId, bankEndId);
    zinfo->commMapping->setMapping(level, bankBeginId, bankEndId, commId);
    taskUnit->setCommModule(this);
    assert(taskUnit->getTaskUnitId() == this->commId);;
    this->toStealSize = 0;
}

void BottomCommModule::gatherState() {
    this->taskUnit->computeExecuteSpeed();
    this->executeSpeed = this->taskUnit->getExecuteSpeed();
}

CommPacket* BottomCommModule::nextPacket(uint32_t fromLevel, uint32_t fromCommId, 
                                         uint32_t sizeLimit) {
    CommPacketQueue* cpd;
    if (fromLevel == 0) {
        cpd = &(this->siblingPackets[fromCommId-siblingBeginId]);
    } else if (fromLevel == 1) {
        cpd = &(this->parentPackets);
    } else {
        panic("invalid fromLevel %u for nextPacket from BottomCommModule", fromLevel);
    }
    CommPacket* ret = cpd->front();
    if (ret != nullptr && ret->getSize() < sizeLimit) {
        cpd->pop();
        return ret;
    }
    return nullptr;
}

void BottomCommModule::executeLoadBalance(
        const LbCommand& command, uint32_t targetBankId, 
        std::vector<DataHotness>& outInfo) {
    DEBUG_LB_O("%s execute load balance", this->getName());
    assert(targetBankId == this->commId);
    this->taskUnit->getCurUnit()->executeLoadBalanceCommand(command, outInfo);
}

void BottomCommModule::handleInPacket(CommPacket* packet) {
    assert_msg(packet->fromLevel == 1 && packet->toLevel == 0, "fromLevel: %u, toLevel: %u", 
        packet->fromLevel, packet->toLevel);
    assert(packet->toCommId >= 0 && (uint32_t)packet->toCommId == this->commId);
    // DEBUG_SCHED_META_O("module %s handle in packet type: %u addr: %lu sig: %lu, idx: %u", 
    //     this->getName(), packet->type, packet->getAddr(), 
    //     packet->getSignature(), packet->getIdx());
    if (packet->getInnerType() == CommPacket::PacketType::DataLend) {
        int avail = this->checkAvailable(packet->getAddr());
        if (avail == -1 || avail == 0) { 
            delete packet;
            s_RecvPackets.atomicInc(1);
            return;
        }
    }
    if (packet->type == CommPacket::PacketType::Sub) {
        auto p = (SubCommPacket*) packet; 
        if (p->parent->type == CommPacket::PacketType::DataLend) {
            this->addrRemapTable->setAddrBorrowMidState(p->getAddr(), p->idx);
        }
        if (p->isLast()) {
            p->parent->fromLevel = p->fromLevel;
            p->parent->fromCommId = p->fromCommId;
            p->parent->toLevel = p->toLevel;
            p->parent->toCommId = p->toCommId;
            this->handleInPacket(p->parent);
            delete packet;
            return; // should not add recvPackets, since we have done it for the parent packet
        }
    } else if (packet->type == CommPacket::PacketType::Task) {
        int avail = this->checkAvailable(packet->getAddr());
        if (avail == -1) {
            this->handleOutPacket(packet);
            return;
        } else {
            auto p = (TaskCommPacket*) packet;
            if (p->forLb()) {
                this->taskUnit->setHasReceiveLbTask(true);
                this->s_ScheduleInTasks.atomicInc(1); 
                DEBUG_LB_O("module %s receive lb task from bank %u, addr: %lu, taskId: %lu, toSteal: %lu", 
                    this->getName(), p->fromCommId, p->getAddr(), p->task->taskId, this->toStealSize);
                if (this->toStealSize >= 1) {
                    --this->toStealSize;
                }
            } else {
                DEBUG_TASK_BEHAVIOR_O("module %s receive migrated task from bank %u, addr: %lu, taskId: %lu", 
                    this->getName(), p->fromCommId, p->getAddr(), p->task->taskId);
            }
            p->task->readyCycle = p->readyCycle;
            this->taskUnit->taskEnqueue(p->task, avail);
        }
    } else if (packet->type == CommPacket::PacketType::DataLend) {
        auto p = (DataLendCommPacket*) packet;
        this->newAddrRemap(p->getAddr(), 0, false);
        this->taskUnit->newAddrBorrow(p->getAddr());
    } else {
        panic("Invalid packet type %u for BottomCommModule!", packet->type);
    }
    delete packet;
    s_RecvPackets.atomicInc(1);
}

int BottomCommModule::checkAvailable(Address lbPageAddr) {
    Address pageAddr = zinfo->numaMap->getPageAddressFromLbPageAddress(lbPageAddr);
    uint32_t dataNodeId = zinfo->numaMap->getNodeOfPage(pageAddr);
    
    // Check address remapping first
    int remap = this->addrRemapTable->getChildRemap(lbPageAddr);
    if (remap != -1) {
        assert(dataNodeId != this->commId && !this->addrRemapTable->getAddrBorrowMidState(lbPageAddr));
        return 0;  // Data is remapped and available
    }
    
    // Check if data is in borrowing state (transfer in progress)
    if (this->addrRemapTable->getAddrBorrowMidState(lbPageAddr)) {
        return -2;  // Transfer in progress
    }
    
    // Enhanced availability check with node type awareness
    if (zinfo->TASK_BASED && zinfo->enableComputeRelocation) {
        if (dataNodeId == this->commId) {
            // Data is on local node
            if (zinfo->isActiveNode[this->commId]) {
                // Local node is active, check if data is not lent out
                return !this->addrRemapTable->getAddrLend(lbPageAddr) ? 0 : -1;
            } else {
                // Local node is storage node - this should not happen
                // as tasks should only execute on active nodes
                panic("Task should not be executed on storage node %d", this->commId);
            }
        } else {
            // Data is on remote node
            if (!zinfo->isActiveNode[dataNodeId]) {
                // Data is on a storage node, check if current node is responsible for it
                if (zinfo->storageToActiveMap[dataNodeId] == this->commId) {
                    // Current active node is responsible for this storage node
                    return !this->addrRemapTable->getAddrLend(lbPageAddr) ? 0 : -1;
                }
            }
            // Data is on other active node or storage node managed by other active node
            return -1;  // Data not available locally
        }
    } else {
        // Fallback to original logic if compute relocation is disabled
        if (dataNodeId == this->commId && !this->addrRemapTable->getAddrLend(lbPageAddr)) {
            return 0;
        } else {
            return -1;
        }
    }
}

void BottomCommModule::pushDataLendPackets() {
    for (auto item : toLendMap) {
        this->handleOutPacket(item.second);
    }
    this->toLendMap.clear();
}

void BottomCommModule::initStats(AggregateStat* parentStat) {
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

    s_ScheduleOutData.init("scheduleOutData", "Number of scheduled out data");
    commStat->append(&s_ScheduleOutData);
    s_ScheduleInData.init("scheduleInData", "Number of scheduled in data");
    commStat->append(&s_ScheduleInData);
    // s_InAndOutData.init("inAndOutData", "Number of scheduled in then out data");
    // tuStat->append(&s_InAndOutData);
    // s_OutAndInData.init("outAndInData", "Number of scheduled out then in data");
    // tuStat->append(&s_OutAndInData);
    s_ScheduleOutTasks.init("scheduleOutTasks", "Number of scheduled out tasks");
    commStat->append(&s_ScheduleOutTasks);
    s_ScheduleInTasks.init("scheduleInTasks", "Number of scheduled in tasks");
    commStat->append(&s_ScheduleInTasks);

    parentStat->append(commStat);
}