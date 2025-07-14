#pragma once
#include <vector>
#include <unordered_map>
#include "config.h"

namespace task_support {

enum class BankType {
    ACTIVE,    // 计算Bank，参与任务执行和负载均衡计算
    STORAGE    // 存储Bank，仅存储数据，队列长度始终为0
};

class BankTypeManager {
private:
    std::vector<BankType> bankTypes;
    std::vector<uint32_t> activeBankIds;
    std::vector<uint32_t> storageBankIds;
    std::unordered_map<uint32_t, uint32_t> storageToActiveMapping; // 存储Bank -> 负责的计算Bank
    
    uint32_t totalBanks;
    uint32_t activeBankCount;
    uint32_t storageBankCount;
    
    // 配置参数
    uint32_t activeNodeRatio;
    uint32_t storageNodeRatio;
    
public:
    BankTypeManager();
    
    // 初始化Bank类型配置
    void initialize(Config& config, uint32_t numBanks);
    void initializeFromRatio(uint32_t numBanks, uint32_t activeRatio, uint32_t storageRatio);
    void initializeFromConfig(Config& config, uint32_t numBanks);
    
    // Bank类型查询
    BankType getBankType(uint32_t bankId) const;
    bool isActiveBank(uint32_t bankId) const;
    bool isStorageBank(uint32_t bankId) const;
    
    // Bank列表获取
    const std::vector<uint32_t>& getActiveBankIds() const { return activeBankIds; }
    const std::vector<uint32_t>& getStorageBankIds() const { return storageBankIds; }
    std::vector<bool> getActiveBankFlags() const;
    std::vector<bool> getStorageBankFlags() const;
    
    // 存储Bank映射管理
    uint32_t getResponsibleActiveBank(uint32_t storageBankId) const;
    void setStorageToActiveMapping(uint32_t storageBankId, uint32_t activeBankId);
    
    // 统计信息
    uint32_t getTotalBanks() const { return totalBanks; }
    uint32_t getActiveBankCount() const { return activeBankCount; }
    uint32_t getStorageBankCount() const { return storageBankCount; }
    
    // 负载计算相关：排除存储Bank
    std::vector<uint32_t> filterActiveBankQueueLengths(const std::vector<uint32_t>& allQueueLengths) const;
    double calculateActiveAverageLoad(const std::vector<uint32_t>& allQueueLengths) const;
    
    // 调试和日志
    void printConfiguration() const;
    std::string getBankTypeString(uint32_t bankId) const;
    
private:
    void setupActiveBanks();
    void setupStorageBanks();
    void setupDefaultMapping();
};

} // namespace task_support
