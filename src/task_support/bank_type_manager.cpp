#include "bank_type_manager.h"
#include "log.h"
#include <algorithm>
#include <cassert>

namespace task_support {

BankTypeManager::BankTypeManager() 
    : totalBanks(0), activeBankCount(0), storageBankCount(0), 
      activeNodeRatio(1), storageNodeRatio(0) {
}

void BankTypeManager::initialize(Config& config, uint32_t numBanks) {
    totalBanks = numBanks;
    
    // 读取配置参数
    activeNodeRatio = config.get<uint32_t>("sys.taskSupport.activeNodeRatio", 1);
    storageNodeRatio = config.get<uint32_t>("sys.taskSupport.storageNodeRatio", 0);
    
    info("Initializing BankTypeManager: totalBanks=%u, activeRatio=%u, storageRatio=%u", 
         totalBanks, activeNodeRatio, storageNodeRatio);
    
    initializeFromRatio(numBanks, activeNodeRatio, storageNodeRatio);
    printConfiguration();
}

void BankTypeManager::initializeFromRatio(uint32_t numBanks, uint32_t activeRatio, uint32_t storageRatio) {
    totalBanks = numBanks;
    activeNodeRatio = activeRatio;
    storageNodeRatio = storageRatio;
    
    bankTypes.resize(totalBanks);
    activeBankIds.clear();
    storageBankIds.clear();
    storageToActiveMapping.clear();
    
    uint32_t totalRatio = activeRatio + storageRatio;
    
    if (totalRatio == 0) {
        // 默认情况：所有Bank都是计算Bank
        for (uint32_t i = 0; i < totalBanks; i++) {
            bankTypes[i] = BankType::ACTIVE;
            activeBankIds.push_back(i);
        }
        activeBankCount = totalBanks;
        storageBankCount = 0;
    } else {
        // 根据比例分配Bank类型
        for (uint32_t i = 0; i < totalBanks; i++) {
            if ((i % totalRatio) < activeRatio) {
                bankTypes[i] = BankType::ACTIVE;
                activeBankIds.push_back(i);
            } else {
                bankTypes[i] = BankType::STORAGE;
                storageBankIds.push_back(i);
            }
        }
        
        activeBankCount = activeBankIds.size();
        storageBankCount = storageBankIds.size();
        
        // 建立存储Bank到计算Bank的默认映射
        setupDefaultMapping();
    }
}

void BankTypeManager::initializeFromConfig(Config& config, uint32_t numBanks) {
    // 如果配置文件中有明确的Bank类型配置，可以在这里实现
    // 目前使用ratio方式
    initialize(config, numBanks);
}

BankType BankTypeManager::getBankType(uint32_t bankId) const {
    assert(bankId < totalBanks);
    return bankTypes[bankId];
}

bool BankTypeManager::isActiveBank(uint32_t bankId) const {
    return getBankType(bankId) == BankType::ACTIVE;
}

bool BankTypeManager::isStorageBank(uint32_t bankId) const {
    return getBankType(bankId) == BankType::STORAGE;
}

std::vector<bool> BankTypeManager::getActiveBankFlags() const {
    std::vector<bool> flags(totalBanks, false);
    for (uint32_t id : activeBankIds) {
        flags[id] = true;
    }
    return flags;
}

std::vector<bool> BankTypeManager::getStorageBankFlags() const {
    std::vector<bool> flags(totalBanks, false);
    for (uint32_t id : storageBankIds) {
        flags[id] = true;
    }
    return flags;
}

uint32_t BankTypeManager::getResponsibleActiveBank(uint32_t storageBankId) const {
    assert(isStorageBank(storageBankId));
    auto it = storageToActiveMapping.find(storageBankId);
    if (it != storageToActiveMapping.end()) {
        return it->second;
    }
    
    // 如果没有明确映射，返回第一个计算Bank
    return activeBankIds.empty() ? 0 : activeBankIds[0];
}

void BankTypeManager::setStorageToActiveMapping(uint32_t storageBankId, uint32_t activeBankId) {
    assert(isStorageBank(storageBankId));
    assert(isActiveBank(activeBankId));
    storageToActiveMapping[storageBankId] = activeBankId;
}

std::vector<uint32_t> BankTypeManager::filterActiveBankQueueLengths(const std::vector<uint32_t>& allQueueLengths) const {
    std::vector<uint32_t> activeQueueLengths;
    activeQueueLengths.reserve(activeBankCount);
    
    for (uint32_t bankId : activeBankIds) {
        if (bankId < allQueueLengths.size()) {
            activeQueueLengths.push_back(allQueueLengths[bankId]);
        }
    }
    
    return activeQueueLengths;
}

double BankTypeManager::calculateActiveAverageLoad(const std::vector<uint32_t>& allQueueLengths) const {
    if (activeBankCount == 0) return 0.0;
    
    uint64_t totalLoad = 0;
    uint32_t validBanks = 0;
    
    for (uint32_t bankId : activeBankIds) {
        if (bankId < allQueueLengths.size()) {
            totalLoad += allQueueLengths[bankId];
            validBanks++;
        }
    }
    
    return validBanks > 0 ? static_cast<double>(totalLoad) / validBanks : 0.0;
}

void BankTypeManager::printConfiguration() const {
    info("Bank Type Configuration:");
    info("  Total Banks: %u", totalBanks);
    info("  Active Banks: %u", activeBankCount);
    info("  Storage Banks: %u", storageBankCount);
    info("  Active/Storage Ratio: %u:%u", activeNodeRatio, storageNodeRatio);
    
    if (activeBankCount <= 16) { // 只在Bank数量较少时打印详细信息
        std::string activeBankStr = "Active Banks: ";
        for (uint32_t id : activeBankIds) {
            activeBankStr += std::to_string(id) + " ";
        }
        info("  %s", activeBankStr.c_str());
        
        if (storageBankCount > 0) {
            std::string storageBankStr = "Storage Banks: ";
            for (uint32_t id : storageBankIds) {
                storageBankStr += std::to_string(id) + " ";
            }
            info("  %s", storageBankStr.c_str());
        }
    }
}

std::string BankTypeManager::getBankTypeString(uint32_t bankId) const {
    switch (getBankType(bankId)) {
        case BankType::ACTIVE: return "ACTIVE";
        case BankType::STORAGE: return "STORAGE";
        default: return "UNKNOWN";
    }
}

void BankTypeManager::setupDefaultMapping() {
    if (activeBankIds.empty()) return;
    
    // 为每个存储Bank分配一个负责的计算Bank
    // 使用轮询方式分配
    for (size_t i = 0; i < storageBankIds.size(); i++) {
        uint32_t storageBankId = storageBankIds[i];
        uint32_t responsibleActiveBank = activeBankIds[i % activeBankIds.size()];
        storageToActiveMapping[storageBankId] = responsibleActiveBank;
    }
    
    info("Established default storage-to-active bank mapping");
}

} // namespace task_support
