#include "bank_group_load_balancer.h"
#include "comm_support/comm_module.h"
#include "debug.h"
#include <algorithm>
#include <cmath>

namespace pimbridge {

BankGroupLoadBalancer::BankGroupLoadBalancer(Config& config, uint32_t _level, uint32_t _commId)
    : LoadBalancer(config, _level, _commId), numGroups(0), avgLoadFactor(0.0) {
    
    // 读取配置参数
    LOAD_IMBALANCE_THRESHOLD = config.get<double>("sys.pimBridge.loadBalancer.loadImbalanceThreshold", 2.0);
    MIN_DATA_PER_REASSIGNMENT = config.get<uint32_t>("sys.pimBridge.loadBalancer.minDataPerReassignment", 10);
    GROUP_SIZE_TOLERANCE = config.get<double>("sys.pimBridge.loadBalancer.groupSizeTolerance", 0.2);
    
    info("BankGroupLoadBalancer initialized: threshold=%.2f, minData=%u, tolerance=%.2f", 
         LOAD_IMBALANCE_THRESHOLD, MIN_DATA_PER_REASSIGNMENT, GROUP_SIZE_TOLERANCE);
}

void BankGroupLoadBalancer::initializeBankTypes(const std::vector<bool>& activeFlags, 
                                               const std::vector<bool>& storageFlags) {
    uint32_t numBanks = activeFlags.size();
    isActiveBank = activeFlags;
    isStorageBank = storageFlags;
    
    activeBankList.clear();
    for (uint32_t i = 0; i < numBanks; i++) {
        if (isActiveBank[i]) {
            activeBankList.push_back(i);
        }
    }
    
    bankQueueLengths.resize(numBanks, 0);
    bankLoadFactors.resize(numBanks, 0.0);
    bankGroupCommands.resize(numBanks);
    
    info("Initialized bank types: %lu active banks, %lu storage banks", 
         (unsigned long)activeBankList.size(), (unsigned long)(numBanks - activeBankList.size()));
}

void BankGroupLoadBalancer::initializeBankGroups(uint32_t groupCount) {
    numGroups = groupCount;
    bankToGroup.resize(isActiveBank.size(), 0);
    groupToBanks.resize(numGroups);
    
    // 初始化：将计算Bank平均分配到各个Group
    for (size_t i = 0; i < activeBankList.size(); i++) {
        uint32_t bankId = activeBankList[i];
        uint32_t groupId = i % numGroups;
        bankToGroup[bankId] = groupId;
        groupToBanks[groupId].push_back(bankId);
    }
    
    info("Initialized %u bank groups with %u active banks", numGroups, (uint32_t)activeBankList.size());
}

void BankGroupLoadBalancer::updateBankLoads(const std::vector<uint32_t>& queueLengths) {
    bankQueueLengths = queueLengths;
    
    // 计算负载因子（仅考虑计算Bank）
    double totalLoad = 0.0;
    uint32_t activeCount = 0;
    
    for (uint32_t bankId : activeBankList) {
        double loadFactor = calculateLoadFactor(bankId);
        bankLoadFactors[bankId] = loadFactor;
        totalLoad += loadFactor;
        activeCount++;
    }
    
    avgLoadFactor = activeCount > 0 ? totalLoad / activeCount : 0.0;
    
    DEBUG_LB_O("Updated bank loads: avgLoad=%.2f, activeCount=%u", avgLoadFactor, activeCount);
}

void BankGroupLoadBalancer::updateDataOwnership(Address addr, uint32_t bankId) {
    uint32_t oldOwner = dataOwnership[addr];
    if (oldOwner != bankId) {
        // 更新数据所有权
        if (oldOwner != 0) {
            bankOwnedData[oldOwner].erase(addr);
        }
        dataOwnership[addr] = bankId;
        bankOwnedData[bankId].insert(addr);
    }
}

double BankGroupLoadBalancer::calculateLoadFactor(uint32_t bankId) {
    if (isStorageBank[bankId]) {
        return 0.0; // 存储Bank负载始终为0
    }
    
    // 负载因子 = 队列长度 + 管辖数据量权重
    double queueLoad = static_cast<double>(bankQueueLengths[bankId]);
    double dataLoad = static_cast<double>(bankOwnedData[bankId].size()) * 0.1; // 数据权重可配置
    
    return queueLoad + dataLoad;
}

bool BankGroupLoadBalancer::detectLoadImbalance() {
    if (avgLoadFactor == 0.0) return false;
    
    // 检查是否有Bank负载显著超过平均值
    for (uint32_t bankId : activeBankList) {
        double loadFactor = bankLoadFactors[bankId];
        if (loadFactor > avgLoadFactor * LOAD_IMBALANCE_THRESHOLD) {
            DEBUG_LB_O("Detected load imbalance: bank %u load=%.2f, avg=%.2f", 
                      bankId, loadFactor, avgLoadFactor);
            return true;
        }
    }
    
    return false;
}

void BankGroupLoadBalancer::generateCommand(bool* needParentLevelLb) {
    resetCommands();
    
    // 更新负载状态
    // updateBankLoads() 应该在调用此函数前被调用
    
    // 检测负载不均衡
    if (!detectLoadImbalance()) {
        DEBUG_LB_O("No load imbalance detected, skipping rebalancing");
        *needParentLevelLb = false;
        return;
    }
    
    // 生成Bank Group重分配策略
    generateGroupReassignments();
    generateDataReassignments();
    
    // 检查是否需要上级负载均衡
    bool hasCommands = false;
    for (const auto& cmd : bankGroupCommands) {
        if (!cmd.empty()) {
            hasCommands = true;
            break;
        }
    }
    
    *needParentLevelLb = !hasCommands; // 如果本级无法解决，需要上级介入
    
    if (hasCommands) {
        info("Generated bank group load balancing commands");
    }
}

void BankGroupLoadBalancer::generateGroupReassignments() {
    // 识别过载和欠载的Bank
    std::vector<uint32_t> overloadedBanks = selectOverloadedBanks();
    std::vector<uint32_t> underloadedBanks = selectUnderloadedBanks();
    
    if (overloadedBanks.empty() || underloadedBanks.empty()) {
        return;
    }
    
    // Group内重平衡
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        rebalanceWithinGroup(groupId);
    }
    
    // Group间重平衡
    rebalanceBetweenGroups();
}

void BankGroupLoadBalancer::generateDataReassignments() {
    std::vector<uint32_t> overloadedBanks = selectOverloadedBanks();
    
    for (uint32_t overloadedBank : overloadedBanks) {
        const auto& ownedData = bankOwnedData[overloadedBank];
        if (ownedData.size() < MIN_DATA_PER_REASSIGNMENT) {
            continue;
        }
        
        // 选择目标Bank（同Group内的欠载Bank优先）
        uint32_t groupId = bankToGroup[overloadedBank];
        std::vector<uint32_t> candidates;
        
        for (uint32_t bankId : groupToBanks[groupId]) {
            if (bankId != overloadedBank && 
                bankLoadFactors[bankId] < avgLoadFactor * 0.8) {
                candidates.push_back(bankId);
            }
        }
        
        if (candidates.empty()) {
            // 扩展到其他Group
            for (uint32_t bankId : activeBankList) {
                if (bankId != overloadedBank && 
                    bankLoadFactors[bankId] < avgLoadFactor * 0.8) {
                    candidates.push_back(bankId);
                }
            }
        }
        
        if (!candidates.empty()) {
            // 计算需要重分配的数据量
            size_t dataToMove = ownedData.size() / 4; // 移动25%的数据
            dataToMove = std::max(dataToMove, static_cast<size_t>(MIN_DATA_PER_REASSIGNMENT));
            
            auto it = ownedData.begin();
            for (size_t i = 0; i < dataToMove && it != ownedData.end(); i++, it++) {
                uint32_t targetBank = findBestTargetBank(*it, candidates);
                bankGroupCommands[overloadedBank].addDataReassignment(*it, targetBank);
            }
            
            logReassignmentDecision("Data overload", overloadedBank, 0, dataToMove);
        }
    }
}

std::vector<uint32_t> BankGroupLoadBalancer::selectOverloadedBanks() {
    std::vector<uint32_t> overloaded;
    double threshold = avgLoadFactor * LOAD_IMBALANCE_THRESHOLD;
    
    for (uint32_t bankId : activeBankList) {
        if (bankLoadFactors[bankId] > threshold) {
            overloaded.push_back(bankId);
        }
    }
    
    return overloaded;
}

std::vector<uint32_t> BankGroupLoadBalancer::selectUnderloadedBanks() {
    std::vector<uint32_t> underloaded;
    double threshold = avgLoadFactor * 0.5; // 50%平均负载以下视为欠载
    
    for (uint32_t bankId : activeBankList) {
        if (bankLoadFactors[bankId] < threshold) {
            underloaded.push_back(bankId);
        }
    }
    
    return underloaded;
}

uint32_t BankGroupLoadBalancer::findBestTargetBank(Address addr, const std::vector<uint32_t>& candidates) {
    if (candidates.empty()) return 0;
    
    // 选择负载最低的Bank
    uint32_t bestBank = candidates[0];
    double minLoad = bankLoadFactors[bestBank];
    
    for (uint32_t bankId : candidates) {
        if (bankLoadFactors[bankId] < minLoad) {
            minLoad = bankLoadFactors[bankId];
            bestBank = bankId;
        }
    }
    
    return bestBank;
}

void BankGroupLoadBalancer::rebalanceWithinGroup(uint32_t groupId) {
    const auto& groupBanks = groupToBanks[groupId];
    if (groupBanks.size() <= 1) return;
    
    // 计算Group内负载分布
    double groupTotalLoad = 0.0;
    for (uint32_t bankId : groupBanks) {
        groupTotalLoad += bankLoadFactors[bankId];
    }
    
    double groupAvgLoad = groupTotalLoad / groupBanks.size();
    
    // 在Group内重分配过载Bank的数据
    for (uint32_t bankId : groupBanks) {
        if (bankLoadFactors[bankId] > groupAvgLoad * 1.5) {
            // 这个Bank在Group内过载，需要重分配部分数据
            for (uint32_t targetBank : groupBanks) {
                if (targetBank != bankId && bankLoadFactors[targetBank] < groupAvgLoad * 0.8) {
                    // 可以接收数据的目标Bank
                    bankGroupCommands[bankId].addGroupReassignment(bankId, groupId);
                    break;
                }
            }
        }
    }
}

void BankGroupLoadBalancer::rebalanceBetweenGroups() {
    // 计算各Group的总负载
    std::vector<double> groupLoads(numGroups, 0.0);
    std::vector<uint32_t> groupSizes(numGroups, 0);
    
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        for (uint32_t bankId : groupToBanks[groupId]) {
            groupLoads[groupId] += bankLoadFactors[bankId];
            groupSizes[groupId]++;
        }
    }
    
    // 寻找负载差异显著的Group并重分配
    for (uint32_t i = 0; i < numGroups; i++) {
        for (uint32_t j = i + 1; j < numGroups; j++) {
            double loadDiff = std::abs(groupLoads[i] - groupLoads[j]);
            double avgLoad = (groupLoads[i] + groupLoads[j]) / 2.0;
            
            if (loadDiff > avgLoad * 0.5) { // 负载差异超过50%
                // 考虑Group间Bank重分配
                uint32_t sourceGroup = groupLoads[i] > groupLoads[j] ? i : j;
                uint32_t targetGroup = groupLoads[i] > groupLoads[j] ? j : i;
                
                // 选择源Group中负载较低的Bank移动到目标Group
                if (groupToBanks[sourceGroup].size() > 1) {
                    uint32_t bankToMove = 0;
                    double minLoad = std::numeric_limits<double>::max();
                    
                    for (uint32_t bankId : groupToBanks[sourceGroup]) {
                        if (bankLoadFactors[bankId] < minLoad) {
                            minLoad = bankLoadFactors[bankId];
                            bankToMove = bankId;
                        }
                    }
                    
                    if (bankToMove != 0) {
                        bankGroupCommands[bankToMove].addGroupReassignment(bankToMove, targetGroup);
                        logReassignmentDecision("Group rebalance", bankToMove, targetGroup, 0);
                    }
                }
            }
        }
    }
}

void BankGroupLoadBalancer::assignLbTarget(const std::vector<DataHotness>& outInfo) {
    // 处理数据热度信息，用于优化数据分配决策
    for (const auto& hotness : outInfo) {
        Address addr = hotness.addr;
        uint32_t srcBank = hotness.srcBankId;
        uint32_t accessCount = hotness.cnt;
        
        // 如果数据访问频率很高，考虑将其重分配到访问它的Bank
        if (accessCount > 10) { // 可配置阈值
            updateDataOwnership(addr, srcBank);
        }
    }
}

void BankGroupLoadBalancer::resetCommands() {
    for (auto& cmd : bankGroupCommands) {
        cmd.reset();
    }
}

void BankGroupLoadBalancer::logReassignmentDecision(const std::string& reason, 
                                                   uint32_t fromBank, 
                                                   uint32_t toBank, 
                                                   size_t dataCount) {
    DEBUG_LB_O("Reassignment decision: %s, from bank %u to bank %u, data count %zu", 
              reason.c_str(), fromBank, toBank, dataCount);
}

} // namespace pimbridge
