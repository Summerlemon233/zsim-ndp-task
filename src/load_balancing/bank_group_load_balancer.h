#pragma once
#include "load_balancer.h"
#include <unordered_set>
#include <unordered_map>

namespace pimbridge {

// 存储Bank重分配命令
class StorageBankReassignment {
public:
    uint32_t storageBankId;
    uint32_t sourceGroupId;
    uint32_t targetGroupId;
    uint32_t taskCount;
    
    StorageBankReassignment(uint32_t storage, uint32_t source, uint32_t target, uint32_t tasks)
        : storageBankId(storage), sourceGroupId(source), targetGroupId(target), taskCount(tasks) {}
};

// 任务迁移结构
struct TaskMigration {
    uint32_t sourceBankId;
    uint32_t targetBankId;
    uint32_t taskCount;
    std::vector<Address> dataAddresses;
    
    TaskMigration(uint32_t src, uint32_t tgt, uint32_t count) 
        : sourceBankId(src), targetBankId(tgt), taskCount(count) {}
};

// 新的Bank Group重分配命令，用于数据接管机制
class BankGroupCommand {
private:
    std::vector<uint32_t> groupReassignments; // Bank -> 新的Group ID
    std::vector<StorageBankReassignment> storageBankReassignments; // 存储Bank重分配列表
    std::vector<TaskMigration> taskMigrations; // 任务迁移列表
    
public:
    BankGroupCommand() {}
    void reset() { 
        groupReassignments.clear();
        storageBankReassignments.clear();
        taskMigrations.clear();
    }
    
    // 添加Bank Group重分配命令
    void addGroupReassignment(uint32_t bankId, uint32_t newGroupId) {
        if (groupReassignments.size() <= bankId) {
            groupReassignments.resize(bankId + 1, UINT32_MAX);
        }
        groupReassignments[bankId] = newGroupId;
    }
    
    // 添加存储Bank重分配命令
    void addStorageBankReassignment(const StorageBankReassignment& reassignment) {
        storageBankReassignments.push_back(reassignment);
    }
    
    const std::vector<uint32_t>& getGroupReassignments() const {
        return groupReassignments;
    }
    
    const std::vector<StorageBankReassignment>& getStorageBankReassignments() const {
        return storageBankReassignments;
    }
    
    // 添加任务迁移命令
    void addTaskMigration(uint32_t sourceBankId, uint32_t targetBankId, uint32_t taskCount) {
        taskMigrations.emplace_back(sourceBankId, targetBankId, taskCount);
    }
    
    const std::vector<TaskMigration>& getTaskMigrations() const {
        return taskMigrations;
    }
    
    bool empty() const { 
        return groupReassignments.empty() && 
               storageBankReassignments.empty() && taskMigrations.empty(); 
    }
    
    std::string output() {
        if (empty()) return "None";
        std::stringstream ss;
        ss << "GroupReassignments: " << groupReassignments.size()
           << ", StorageBankReassignments: " << storageBankReassignments.size()
           << ", TaskMigrations: " << taskMigrations.size();
        return ss.str();
    }
};

// Bank Group重分配负载均衡器
class BankGroupLoadBalancer : public LoadBalancer {
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
    
    // 新增：任务分类统计和负载计算
    void updateBankTaskClassification(const std::vector<uint32_t>& queueLengths);
    double calculateGroupLoad(uint32_t groupId);
    uint32_t getTasksForDataOnBank(uint32_t bankId);
    void validateTaskAccounting(); // 基础验证
    
    // 访问接口
    const BankGroupCommand& getBankGroupCommand(uint32_t bankIndex) const {
        if (bankIndex >= bankGroupCommands.size()) {
            static BankGroupCommand emptyCommand;
            return emptyCommand;
        }
        return bankGroupCommands[bankIndex];
    }
    
    // Bank Group信息查询接口
    uint32_t getBankGroupId(uint32_t bankId) const {
        return bankId < bankToGroup.size() ? bankToGroup[bankId] : 0;
    }
    
    uint32_t getActiveBankForGroup(uint32_t groupId) const {
        return findActiveBankInGroup(groupId);
    }
    
    const std::vector<uint32_t>& getBanksInGroup(uint32_t groupId) const {
        static std::vector<uint32_t> empty;
        return groupId < groupToBanks.size() ? groupToBanks[groupId] : empty;
    }
    
    // Bank Group重分配核心算法
    bool detectLoadImbalance();
    void generateGroupReassignments();
    void optimizeBankGroupMapping();
    
    // 存储Bank重分配核心算法
    void executeStorageBankReassignments();
    void reassignStorageBankToGroup(uint32_t storageBankId, uint32_t sourceGroupId, uint32_t targetGroupId);
    void updateStorageBankMapping(uint32_t storageBankId, uint32_t newGroupId);
    void redistributeStorageBankTasks(uint32_t storageBankId, uint32_t sourceActiveBank, uint32_t targetActiveBank);
    
    // 任务迁移核心算法
    void generateTaskMigrations();
    void executeTaskMigrations();
    void migrateTasksBetweenBanks(uint32_t sourceBankId, uint32_t targetBankId, uint32_t taskCount);
    std::vector<Address> selectTasksForMigration(uint32_t sourceBankId, uint32_t taskCount);
    
    // 辅助函数
    double calculateLoadFactor(uint32_t bankId);
    std::vector<uint32_t> selectOverloadedBanks();
    std::vector<uint32_t> selectUnderloadedBanks();
    uint32_t findBestTargetBank(Address addr, const std::vector<uint32_t>& candidates);
    void rebalanceWithinGroup(uint32_t groupId);
    void rebalanceBetweenGroups();
    
    // 新增：Bank Group相关辅助函数
    std::vector<uint32_t> selectOverloadedGroups();
    std::vector<uint32_t> selectUnderloadedGroups();
    uint32_t findActiveBankInGroup(uint32_t groupId) const;
    uint32_t selectHeaviestStorageBank(uint32_t groupId);
    uint32_t selectLightestGroup(const std::vector<uint32_t>& candidates);
    float calculateBankGroupLoad(uint32_t groupId) const;

private:
    // 配置参数
    float loadImbalanceThreshold;        // 负载不均衡阈值
    float taskMigrationThreshold;        // 任务迁移阈值
    uint32_t minTasksForMigration;       // 最小迁移任务数
    uint32_t maxTasksPerMigration;       // 每次迁移最大任务数
    bool enableTaskMigration;            // 是否启用任务迁移
    bool enableStorageBankReassignment;  // 是否启用存储Bank重分配
    double LOAD_IMBALANCE_THRESHOLD;     // 负载不均衡阈值
    uint32_t MIN_DATA_PER_REASSIGNMENT;  // 最小重分配数据量
    double GROUP_SIZE_TOLERANCE;         // Group大小容忍度
    
    // 统计信息
    uint32_t totalLoadBalanceOperations;
    uint32_t totalTaskMigrations;
    uint32_t totalStorageBankReassignments;
    // 架构状态
    uint32_t totalBanks;                 // 总Bank数量
    uint32_t numGroups;                  // Bank Group数量
    uint32_t storageToComputeRatio;      // 存储Bank与计算Bank的比例
    
    // Bank类型管理
    std::vector<bool> isActiveBank;      // 标记哪些Bank是计算Bank
    std::vector<bool> isStorageBank;     // 标记哪些Bank是存储Bank
    std::vector<uint32_t> activeBankList; // 仅包含计算Bank的列表
    
    // Bank Group映射
    std::vector<uint32_t> bankToGroup;           // Bank -> Group映射
    std::vector<std::vector<uint32_t>> groupToBanks; // Group -> Banks映射
    
    // 任务分类统计
    std::vector<uint32_t> localDataTaskCount;    // 每个计算Bank的本地任务数
    std::vector<uint32_t> storageDataTaskCount;  // 每个存储Bank的任务数
    std::vector<std::unordered_map<uint32_t, uint32_t>> managedDataTaskCount; // 每个计算Bank托管的存储Bank任务
    
    // 负载监控
    std::vector<uint32_t> bankQueueLengths;  // 各Bank队列长度
    std::vector<double> bankLoadFactors;     // 各Bank负载因子
    double avgLoadFactor;                    // 平均负载因子
    
    // 命令缓存
    std::vector<BankGroupCommand> bankGroupCommands;
    
    // Phase 4: 验证和调试支持
    void validateBankGroupConfiguration();
    void printLoadBalancingStatistics();
    void printBankGroupMappings();
    void validateTaskCounts();
    void initializeConfigParameters(Config& config);
    
    void resetCommands();
    void logReassignmentDecision(const std::string& reason, uint32_t fromBank, uint32_t toBank, size_t dataCount);
};

} // namespace pimbridge
