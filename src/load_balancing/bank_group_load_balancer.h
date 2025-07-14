#pragma once
#include "load_balancer.h"
#include <unordered_set>
#include <unordered_map>

namespace pimbridge {

// 新的Bank Group重分配命令，用于数据接管机制
class BankGroupCommand {
private:
    std::unordered_map<Address, uint32_t> dataReassignments; // 地址 -> 新负责Bank
    std::vector<uint32_t> groupReassignments; // Bank -> 新的Group ID
    
public:
    BankGroupCommand() {}
    void reset() { 
        dataReassignments.clear(); 
        groupReassignments.clear();
    }
    
    // 添加数据重分配命令
    void addDataReassignment(Address addr, uint32_t newBankId) {
        dataReassignments[addr] = newBankId;
    }
    
    // 添加Bank Group重分配命令
    void addGroupReassignment(uint32_t bankId, uint32_t newGroupId) {
        if (groupReassignments.size() <= bankId) {
            groupReassignments.resize(bankId + 1, UINT32_MAX);
        }
        groupReassignments[bankId] = newGroupId;
    }
    
    const std::unordered_map<Address, uint32_t>& getDataReassignments() const {
        return dataReassignments;
    }
    
    const std::vector<uint32_t>& getGroupReassignments() const {
        return groupReassignments;
    }
    
    bool empty() const { 
        return dataReassignments.empty() && groupReassignments.empty(); 
    }
    
    std::string output() {
        if (empty()) return "None";
        std::stringstream ss;
        ss << "DataReassignments: " << dataReassignments.size() 
           << ", GroupReassignments: " << groupReassignments.size();
        return ss.str();
    }
};

// Bank Group重分配负载均衡器
class BankGroupLoadBalancer : public LoadBalancer {
private:
    // Bank类型管理
    std::vector<bool> isActiveBank;      // 标记哪些Bank是计算Bank
    std::vector<bool> isStorageBank;     // 标记哪些Bank是存储Bank
    std::vector<uint32_t> activeBankList; // 仅包含计算Bank的列表
    
    // Bank Group管理
    std::vector<uint32_t> bankToGroup;   // Bank -> Group映射
    std::vector<std::vector<uint32_t>> groupToBanks; // Group -> Banks映射
    uint32_t numGroups;
    
    // 负载监控
    std::vector<uint32_t> bankQueueLengths;  // 各Bank队列长度
    std::vector<double> bankLoadFactors;     // 各Bank负载因子
    double avgLoadFactor;                    // 平均负载因子
    
    // 数据管辖权映射
    std::unordered_map<Address, uint32_t> dataOwnership; // 地址 -> 负责Bank
    std::unordered_map<uint32_t, std::unordered_set<Address>> bankOwnedData; // Bank -> 管辖数据集
    
    // 重分配命令
    std::vector<BankGroupCommand> bankGroupCommands;
    
    // 配置参数
    double LOAD_IMBALANCE_THRESHOLD;     // 负载不均衡阈值
    uint32_t MIN_DATA_PER_REASSIGNMENT;  // 最小重分配数据量
    double GROUP_SIZE_TOLERANCE;         // Group大小容忍度
    
public:
    BankGroupLoadBalancer(Config& config, uint32_t _level, uint32_t _commId);
    
    // 重写父类虚函数
    void generateCommand(bool* needParentLevelLb) override;
    void assignLbTarget(const std::vector<DataHotness>& outInfo) override;
    
    // 新增Bank Group管理接口
    void initializeBankTypes(const std::vector<bool>& activeFlags, 
                           const std::vector<bool>& storageFlags);
    void initializeBankGroups(uint32_t groupCount);
    void updateBankLoads(const std::vector<uint32_t>& queueLengths);
    void updateDataOwnership(Address addr, uint32_t bankId);
    
    // 访问接口
    const BankGroupCommand& getBankGroupCommand(uint32_t bankIndex) const {
        return bankGroupCommands[bankIndex];
    }
    
    // Bank Group重分配核心算法
    bool detectLoadImbalance();
    void generateGroupReassignments();
    void generateDataReassignments();
    void optimizeBankGroupMapping();
    
    // 辅助函数
    double calculateLoadFactor(uint32_t bankId);
    std::vector<uint32_t> selectOverloadedBanks();
    std::vector<uint32_t> selectUnderloadedBanks();
    uint32_t findBestTargetBank(Address addr, const std::vector<uint32_t>& candidates);
    void rebalanceWithinGroup(uint32_t groupId);
    void rebalanceBetweenGroups();
    
private:
    void resetCommands();
    void logReassignmentDecision(const std::string& reason, uint32_t fromBank, uint32_t toBank, size_t dataCount);
};

} // namespace pimbridge
