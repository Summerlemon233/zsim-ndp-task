#include "bank_group_load_balancer.h"
#include "comm_support/comm_module.h"
#include "debug.h"
#include "log.h"
#include <algorithm>
#include <cmath>

namespace pimbridge {

BankGroupLoadBalancer::BankGroupLoadBalancer(Config& config, uint32_t _level, uint32_t _commId)
    : LoadBalancer(config, _level, _commId), numGroups(0), avgLoadFactor(0.0) {
    
    // Read configuration parameters
    LOAD_IMBALANCE_THRESHOLD = config.get<double>("sys.pimBridge.loadBalancer.loadImbalanceThreshold", 2.0);
    MIN_DATA_PER_REASSIGNMENT = config.get<uint32_t>("sys.pimBridge.loadBalancer.minDataPerReassignment", 10);
    GROUP_SIZE_TOLERANCE = config.get<double>("sys.pimBridge.loadBalancer.groupSizeTolerance", 0.2);
    
    // Initialize new configuration parameters
    initializeConfigParameters(config);
    
    info("BankGroupLoadBalancer initialized: threshold=%.2f, minData=%u, tolerance=%.2f", 
         LOAD_IMBALANCE_THRESHOLD, MIN_DATA_PER_REASSIGNMENT, GROUP_SIZE_TOLERANCE);
    
    info("Task migration: enabled=%s, threshold=%.2f, minTasks=%u, maxTasks=%u",
         enableTaskMigration ? "true" : "false", taskMigrationThreshold, 
         minTasksForMigration, maxTasksPerMigration);
}

void BankGroupLoadBalancer::initializeBankTypes(const std::vector<bool>& activeFlags, 
                                               const std::vector<bool>& storageFlags) {
    totalBanks = activeFlags.size();
    isActiveBank = activeFlags;
    isStorageBank = storageFlags;
    
    activeBankList.clear();
    for (uint32_t i = 0; i < totalBanks; i++) {
        if (isActiveBank[i]) {
            activeBankList.push_back(i);
        }
    }
    
    bankQueueLengths.resize(totalBanks, 0);
    bankLoadFactors.resize(totalBanks, 0.0);
    bankGroupCommands.resize(totalBanks);
    
    // Initialize task classification statistics
    localDataTaskCount.resize(totalBanks, 0);
    managedDataTaskCount.resize(totalBanks);
    storageDataTaskCount.resize(totalBanks, 0);
    
    info("Initialized bank types: %lu active banks, %lu storage banks", 
         (unsigned long)activeBankList.size(), (unsigned long)(totalBanks - activeBankList.size()));
}

void BankGroupLoadBalancer::initializeBankGroups(uint32_t groupCount) {
    // Number of bank groups = number of compute banks
    numGroups = activeBankList.size();
    
    if (groupCount != 0 && groupCount != numGroups) {
        warn("Specified groupCount %u differs from active bank count %u, using active bank count", 
             groupCount, numGroups);
    }
    
    // Clear previous mappings (to prevent reinitialization)
    bankToGroup.clear();
    groupToBanks.clear();
    
    bankToGroup.resize(totalBanks, 0);
    groupToBanks.resize(numGroups);
    
    // Each compute bank forms its own group
    for (size_t i = 0; i < activeBankList.size(); i++) {
        uint32_t activeBankId = activeBankList[i];
        uint32_t groupId = i;
        
        // Compute bank as the core of the group
        bankToGroup[activeBankId] = groupId;
        groupToBanks[groupId].push_back(activeBankId);
    }
    
    // Assign storage banks to groups (round-robin)
    size_t groupIndex = 0;
    for (uint32_t bankId = 0; bankId < totalBanks; bankId++) {
        if (isStorageBank[bankId]) {
            uint32_t targetGroup = groupIndex % numGroups;
            bankToGroup[bankId] = targetGroup;
            groupToBanks[targetGroup].push_back(bankId);
            groupIndex++;
        }
    }
    
    info("Initialized %u bank groups with %u active banks", numGroups, (uint32_t)activeBankList.size());
}

void BankGroupLoadBalancer::updateBankLoads(const std::vector<uint32_t>& queueLengths) {
    bankQueueLengths = queueLengths;
    
    // Update task classification statistics
    updateBankTaskClassification(queueLengths);
    
    // Calculate load factor (only for compute banks)
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

void BankGroupLoadBalancer::updateBankTaskClassification(const std::vector<uint32_t>& queueLengths) {
    // Reset statistics
    localDataTaskCount.assign(totalBanks, 0);
    for (auto& managed : managedDataTaskCount) {
        managed.clear();
    }
    storageDataTaskCount.assign(totalBanks, 0);
    
    // Traverse each compute bank's task queue
    for (uint32_t activeBankId : activeBankList) {
        uint32_t totalTasks = queueLengths[activeBankId];
        
        // Get task classification info from the communication module
        // Note: CommModule instance should be obtained in some way
        // Temporary simplified implementation
        uint32_t localTasks = totalTasks * 3 / 10;
        uint32_t managedTasks = totalTasks - localTasks;
        
        localDataTaskCount[activeBankId] = localTasks;
        
        // Assign managed tasks to storage banks in the group
        uint32_t groupId = bankToGroup[activeBankId];
        const auto& groupBanks = groupToBanks[groupId];
        
        std::vector<uint32_t> storageBanksInGroup;
        for (uint32_t bankId : groupBanks) {
            if (isStorageBank[bankId]) {
                storageBanksInGroup.push_back(bankId);
            }
        }
        
        // Evenly distribute managed tasks to storage banks
        if (!storageBanksInGroup.empty()) {
            uint32_t tasksPerStorage = managedTasks / storageBanksInGroup.size();
            uint32_t remainingTasks = managedTasks % storageBanksInGroup.size();
            
            for (size_t i = 0; i < storageBanksInGroup.size(); i++) {
                uint32_t storageBankId = storageBanksInGroup[i];
                uint32_t taskCount = tasksPerStorage + (i < remainingTasks ? 1 : 0);
                
                managedDataTaskCount[activeBankId][storageBankId] = taskCount;
                storageDataTaskCount[storageBankId] += taskCount;
            }
        }
    }
    
    // Basic validation: ensure the sum of classified tasks equals the total number of tasks
    for (uint32_t activeBankId : activeBankList) {
        uint32_t totalClassified = localDataTaskCount[activeBankId];
        for (const auto& managed : managedDataTaskCount[activeBankId]) {
            totalClassified += managed.second;
        }
        
        if (totalClassified != queueLengths[activeBankId]) {
            warn("Task classification inconsistent for bank %u: total %u, classified %u", 
                 activeBankId, queueLengths[activeBankId], totalClassified);
        }
    }
}

double BankGroupLoadBalancer::calculateLoadFactor(uint32_t bankId) {
    if (isStorageBank[bankId]) {
        return 0.0; // Storage bank load is always 0
    }
    
    // Load factor = queue length (removed data ownership component)
    double queueLoad = static_cast<double>(bankQueueLengths[bankId]);
    
    return queueLoad;
}

double BankGroupLoadBalancer::calculateGroupLoad(uint32_t groupId) {
    const auto& groupBanks = groupToBanks[groupId];
    
    // Find the compute bank in the group
    uint32_t activeBankId = 0;
    for (uint32_t bankId : groupBanks) {
        if (isActiveBank[bankId]) {
            activeBankId = bankId;
            break;
        }
    }
    
    if (activeBankId == 0) {
        warn("Bank Group %u has no active bank", groupId);
        return 0.0;
    }
    
    // Group load = compute bank local data tasks + all managed storage bank data tasks
    double groupLoad = localDataTaskCount[activeBankId];
    
    // Add up all storage bank data tasks in the group
    for (uint32_t bankId : groupBanks) {
        if (isStorageBank[bankId]) {
            groupLoad += storageDataTaskCount[bankId];
        }
    }
    
    return groupLoad;
}

uint32_t BankGroupLoadBalancer::getTasksForDataOnBank(uint32_t bankId) {
    if (isActiveBank[bankId]) {
        return localDataTaskCount[bankId];
    } else {
        return storageDataTaskCount[bankId];
    }
}

bool BankGroupLoadBalancer::detectLoadImbalance() {
    if (avgLoadFactor == 0.0) return false;
    
    // Check if any bank group load significantly exceeds the average
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        double groupLoad = calculateGroupLoad(groupId);
        if (groupLoad > avgLoadFactor * LOAD_IMBALANCE_THRESHOLD) {
            info("Detected load imbalance: group %u load=%.2f, avg=%.2f", 
                 groupId, groupLoad, avgLoadFactor);
            return true;
        }
    }
    
    return false;
}

void BankGroupLoadBalancer::validateTaskAccounting() {
    // Basic validation: the number of tasks in each bank group = its compute bank's queue length
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        uint32_t activeBankId = findActiveBankInGroup(groupId);
        if (activeBankId == 0) continue;
        
        double calculatedGroupLoad = calculateGroupLoad(groupId);
        uint32_t actualQueueLength = bankQueueLengths[activeBankId];
        
        if (abs(calculatedGroupLoad - actualQueueLength) > 1.0) {
            warn("Task accounting mismatch: group %u, calculated %.1f, actual %u", 
                 groupId, calculatedGroupLoad, actualQueueLength);
        }
    }
}

uint32_t BankGroupLoadBalancer::findActiveBankInGroup(uint32_t groupId) const {
    if (groupId >= numGroups) return 0;
    
    const auto& groupBanks = groupToBanks[groupId];
    for (uint32_t bankId : groupBanks) {
        if (isActiveBank[bankId]) {
            return bankId;
        }
    }
    return 0;
}

float BankGroupLoadBalancer::calculateBankGroupLoad(uint32_t groupId) const {
    if (groupId >= numGroups) return 0.0f;
    
    uint32_t activeBankId = findActiveBankInGroup(groupId);
    if (activeBankId == 0) return 0.0f;
    
    // Calculate group load = compute bank local tasks + all managed storage bank tasks
    float totalLoad = static_cast<float>(localDataTaskCount[activeBankId]);
    
    // Add managed storage bank tasks
    if (activeBankId < managedDataTaskCount.size()) {
        const auto& managedTasks = managedDataTaskCount[activeBankId];
        for (const auto& pair : managedTasks) {
            totalLoad += static_cast<float>(pair.second);
        }
    }
    
    return totalLoad;
}

std::vector<uint32_t> BankGroupLoadBalancer::selectOverloadedGroups() {
    std::vector<uint32_t> overloadedGroups;
    double threshold = avgLoadFactor * LOAD_IMBALANCE_THRESHOLD;
    
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        double groupLoad = calculateGroupLoad(groupId);
        if (groupLoad > threshold) {
            overloadedGroups.push_back(groupId);
        }
    }
    
    return overloadedGroups;
}

std::vector<uint32_t> BankGroupLoadBalancer::selectUnderloadedGroups() {
    std::vector<uint32_t> underloadedGroups;
    double threshold = avgLoadFactor * 0.5; // Below 50% of average load is considered underloaded
    
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        double groupLoad = calculateGroupLoad(groupId);
        if (groupLoad < threshold) {
            underloadedGroups.push_back(groupId);
        }
    }
    
    return underloadedGroups;
}

uint32_t BankGroupLoadBalancer::selectHeaviestStorageBank(uint32_t groupId) {
    if (groupId >= numGroups) return 0;
    
    const auto& groupBanks = groupToBanks[groupId];
    uint32_t heaviestBank = 0;
    uint32_t maxTasks = 0;
    
    for (uint32_t bankId : groupBanks) {
        if (isStorageBank[bankId]) {
            uint32_t taskCount = storageDataTaskCount[bankId];
            if (taskCount > maxTasks) {
                maxTasks = taskCount;
                heaviestBank = bankId;
            }
        }
    }
    
    return heaviestBank;
}

uint32_t BankGroupLoadBalancer::selectLightestGroup(const std::vector<uint32_t>& candidates) {
    if (candidates.empty()) return 0;
    
    uint32_t lightestGroup = candidates[0];
    double minLoad = calculateGroupLoad(lightestGroup);
    
    for (uint32_t groupId : candidates) {
        double groupLoad = calculateGroupLoad(groupId);
        if (groupLoad < minLoad) {
            minLoad = groupLoad;
            lightestGroup = groupId;
        }
    }
    
    return lightestGroup;
}

void BankGroupLoadBalancer::reassignStorageBankToGroup(uint32_t storageBankId, uint32_t sourceGroupId, uint32_t targetGroupId) {
    // Validate input parameters
    if (storageBankId >= totalBanks || !isStorageBank[storageBankId]) {
        warn("Invalid storage bank ID: %u", storageBankId);
        return;
    }
    
    if (sourceGroupId >= numGroups || targetGroupId >= numGroups) {
        warn("Invalid group IDs: source=%u, target=%u", sourceGroupId, targetGroupId);
        return;
    }
    
    // Update bank-to-group mapping
    updateStorageBankMapping(storageBankId, targetGroupId);
    
    // Reassign tasks on the storage bank
    uint32_t sourceActiveBank = findActiveBankInGroup(sourceGroupId);
    uint32_t targetActiveBank = findActiveBankInGroup(targetGroupId);
    
    if (sourceActiveBank != 0 && targetActiveBank != 0) {
        redistributeStorageBankTasks(storageBankId, sourceActiveBank, targetActiveBank);
    }
    
    info("Successfully reassigned storage bank %u from group %u to group %u", 
         storageBankId, sourceGroupId, targetGroupId);
}

void BankGroupLoadBalancer::updateStorageBankMapping(uint32_t storageBankId, uint32_t newGroupId) {
    uint32_t oldGroupId = bankToGroup[storageBankId];
    
    // If already in the target group, no need to remap
    if (oldGroupId == newGroupId) {
        return;
    }
    
    // Update bank-to-group mapping
    bankToGroup[storageBankId] = newGroupId;
    
    // Remove from old group
    auto& oldGroupBanks = groupToBanks[oldGroupId];
    oldGroupBanks.erase(std::remove(oldGroupBanks.begin(), oldGroupBanks.end(), storageBankId), 
                       oldGroupBanks.end());
    
    // Check if storage bank already exists in the new group (prevent duplicates)
    auto& newGroupBanks = groupToBanks[newGroupId];
    if (std::find(newGroupBanks.begin(), newGroupBanks.end(), storageBankId) == newGroupBanks.end()) {
        newGroupBanks.push_back(storageBankId);
    }
    
    DEBUG_LB_O("Updated storage bank %u mapping: group %u -> %u", 
              storageBankId, oldGroupId, newGroupId);
}

void BankGroupLoadBalancer::redistributeStorageBankTasks(uint32_t storageBankId, uint32_t sourceActiveBank, uint32_t targetActiveBank) {
    // Get the number of tasks on this storage bank
    uint32_t taskCount = storageDataTaskCount[storageBankId];
    
    if (taskCount == 0) {
        return;
    }
    
    // Remove from source compute bank's managed tasks
    auto& sourceManagedTasks = managedDataTaskCount[sourceActiveBank];
    if (sourceManagedTasks.find(storageBankId) != sourceManagedTasks.end()) {
        sourceManagedTasks.erase(storageBankId);
    }
    
    // Add to target compute bank's managed tasks
    managedDataTaskCount[targetActiveBank][storageBankId] = taskCount;
    
    // Update local task count
    if (localDataTaskCount[sourceActiveBank] >= taskCount) {
        localDataTaskCount[sourceActiveBank] -= taskCount;
    }
    
    info("Redistributed %u tasks from storage bank %u: source active bank %u -> target active bank %u", 
         taskCount, storageBankId, sourceActiveBank, targetActiveBank);
}

void BankGroupLoadBalancer::executeStorageBankReassignments() {
    // Traverse all compute banks and execute storage bank reassignments
    for (uint32_t activeBankId : activeBankList) {
        const auto& command = bankGroupCommands[activeBankId];
        const auto& storageBankReassignments = command.getStorageBankReassignments();
        
        if (storageBankReassignments.empty()) {
            continue;
        }
        
        info("Executing %zu storage bank reassignments for active bank %u", 
             storageBankReassignments.size(), activeBankId);
        
        for (const auto& reassignment : storageBankReassignments) {
            reassignStorageBankToGroup(reassignment.storageBankId, 
                                     reassignment.sourceGroupId, 
                                     reassignment.targetGroupId);
        }
    }
}

void BankGroupLoadBalancer::generateCommand(bool* needParentLevelLb) {
    resetCommands();
    
    // Update load status
    // updateBankLoads() should be called before this function
    
    // Phase 4: Validate configuration and initial state
    validateBankGroupConfiguration();
    totalLoadBalanceOperations++;
    
    // Perform basic validation
    validateTaskAccounting();
    
    // Detect load imbalance
    if (!detectLoadImbalance()) {
        info("No Bank Group load imbalance detected, skipping rebalancing");
        *needParentLevelLb = false;
        return;
    }
    
    // Output current load status
    info("Bank Group Load Balancing - Current state:");
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        double groupLoad = calculateGroupLoad(groupId);
        uint32_t activeBankId = findActiveBankInGroup(groupId);
        info("  Group %u (Active Bank %u): load=%.1f, local=%u, managed=%u", 
             groupId, activeBankId, groupLoad, 
             localDataTaskCount[activeBankId], 
             bankQueueLengths[activeBankId] - localDataTaskCount[activeBankId]);
    }
    
    // Generate Bank Group reassignment strategy
    generateGroupReassignments();
    
    // Generate task migration strategy
    if (enableTaskMigration) {
        generateTaskMigrations();
        totalTaskMigrations++;
    }
    
    // Execute storage bank reassignment
    if (enableStorageBankReassignment) {
        executeStorageBankReassignments();
        totalStorageBankReassignments++;
    }
    
    // Execute task migrations
    if (enableTaskMigration) {
        executeTaskMigrations();
    }
    
    // Check if parent level load balancing is needed
    bool hasCommands = false;
    for (const auto& cmd : bankGroupCommands) {
        if (!cmd.empty()) {
            hasCommands = true;
            break;
        }
    }
    
    *needParentLevelLb = !hasCommands; // If this level cannot solve, parent level is needed
    
    if (hasCommands) {
        info("Generated Bank Group load balancing commands");
        
        // Phase 4: Post-balancing validation and statistics
        printLoadBalancingStatistics();
        printBankGroupMappings();
        validateTaskCounts();
    } else {
        info("No Bank Group rebalancing commands generated");
    }
}

void BankGroupLoadBalancer::generateGroupReassignments() {
    // Identify overloaded and underloaded Bank Groups
    std::vector<uint32_t> overloadedGroups = selectOverloadedGroups();
    std::vector<uint32_t> underloadedGroups = selectUnderloadedGroups();
    
    if (overloadedGroups.empty() || underloadedGroups.empty()) {
        info("No suitable groups for reassignment: overloaded=%zu, underloaded=%zu", 
             overloadedGroups.size(), underloadedGroups.size());
        return;
    }
    
    info("Found %zu overloaded groups and %zu underloaded groups", 
         overloadedGroups.size(), underloadedGroups.size());
    
    // For each overloaded group, try to reassign its heaviest storage bank
    for (uint32_t overloadedGroup : overloadedGroups) {
        uint32_t heaviestStorageBank = selectHeaviestStorageBank(overloadedGroup);
        if (heaviestStorageBank == 0) continue;
        
        uint32_t taskCount = storageDataTaskCount[heaviestStorageBank];
        if (taskCount < MIN_DATA_PER_REASSIGNMENT) {
            continue;
        }
        
        uint32_t targetGroup = selectLightestGroup(underloadedGroups);
        if (targetGroup == overloadedGroup) continue;
        
        // Create storage bank reassignment command
        StorageBankReassignment reassignment(heaviestStorageBank, overloadedGroup, targetGroup, taskCount);
        
        // Execute storage bank reassignment
        info("Reassigning storage bank %u (tasks=%u) from group %u to group %u", 
             heaviestStorageBank, taskCount, overloadedGroup, targetGroup);
        
        reassignStorageBankToGroup(heaviestStorageBank, overloadedGroup, targetGroup);
        
        // Generate reassignment command
        uint32_t sourceActiveBank = findActiveBankInGroup(overloadedGroup);
        bankGroupCommands[sourceActiveBank].addStorageBankReassignment(reassignment);
        
        // Update statistics
        storageDataTaskCount[heaviestStorageBank] = 0; // Will be redistributed in the new group
        
        // Remove the assigned target group from underloaded list to avoid duplicate assignment
        underloadedGroups.erase(std::remove(underloadedGroups.begin(), underloadedGroups.end(), targetGroup), 
                               underloadedGroups.end());
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
    double threshold = avgLoadFactor * 0.5; // Below 50% of average load is considered underloaded
    
    for (uint32_t bankId : activeBankList) {
        if (bankLoadFactors[bankId] < threshold) {
            underloaded.push_back(bankId);
        }
    }
    
    return underloaded;
}

uint32_t BankGroupLoadBalancer::findBestTargetBank(Address addr, const std::vector<uint32_t>& candidates) {
    if (candidates.empty()) return 0;
    
    // Select the bank with the lowest load
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
    
    // Calculate load distribution within the group
    double groupTotalLoad = 0.0;
    for (uint32_t bankId : groupBanks) {
        groupTotalLoad += bankLoadFactors[bankId];
    }
    
    double groupAvgLoad = groupTotalLoad / groupBanks.size();
    
    // Redistribute data from overloaded banks within the group
    for (uint32_t bankId : groupBanks) {
        if (bankLoadFactors[bankId] > groupAvgLoad * 1.5) {
            // This bank is overloaded within the group and needs to redistribute some data
            for (uint32_t targetBank : groupBanks) {
                if (targetBank != bankId && bankLoadFactors[targetBank] < groupAvgLoad * 0.8) {
                    // Target bank can receive data
                    bankGroupCommands[bankId].addGroupReassignment(bankId, groupId);
                    break;
                }
            }
        }
    }
}

void BankGroupLoadBalancer::rebalanceBetweenGroups() {
    // Calculate total load for each group
    std::vector<double> groupLoads(numGroups, 0.0);
    std::vector<uint32_t> groupSizes(numGroups, 0);
    
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        for (uint32_t bankId : groupToBanks[groupId]) {
            groupLoads[groupId] += bankLoadFactors[bankId];
            groupSizes[groupId]++;
        }
    }
    
    // Find groups with significant load differences and redistribute
    for (uint32_t i = 0; i < numGroups; i++) {
        for (uint32_t j = i + 1; j < numGroups; j++) {
            double loadDiff = std::abs(groupLoads[i] - groupLoads[j]);
            double avgLoad = (groupLoads[i] + groupLoads[j]) / 2.0;
            
            if (loadDiff > avgLoad * 0.5) { // Load difference exceeds 50%
                // Consider moving a bank from the source group to the target group
                uint32_t sourceGroup = groupLoads[i] > groupLoads[j] ? i : j;
                uint32_t targetGroup = groupLoads[i] > groupLoads[j] ? j : i;
                
                // Select the bank with the lowest load in the source group to move to the target group
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
    // Process data hotness information for future optimization
    // For now, we skip data reassignment and focus on Bank Group remapping and task migration
    info("Processed %zu data hotness entries (data reassignment disabled)", outInfo.size());
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
    info("Reassignment decision: %s, from bank %u to bank %u, data count %zu", 
         reason.c_str(), fromBank, toBank, dataCount);
}

// ===== Phase 3: Task Migration Implementation =====

void BankGroupLoadBalancer::generateTaskMigrations() {
    info("Starting task migration generation");
    
    // Identify overloaded and underloaded bank pairs
    std::vector<uint32_t> overloadedBanks;
    std::vector<uint32_t> underloadedBanks;
    
    float avgLoad = 0.0f;
    for (uint32_t bankId : activeBankList) {
        avgLoad += calculateBankGroupLoad(bankToGroup[bankId]);
    }
    avgLoad /= activeBankList.size();
    
    // Classify overloaded and underloaded banks
    for (uint32_t bankId : activeBankList) {
        float bankLoad = calculateBankGroupLoad(bankToGroup[bankId]);
        if (bankLoad > avgLoad * 1.2f) { // Overload threshold: 1.2x average load
            overloadedBanks.push_back(bankId);
        } else if (bankLoad < avgLoad * 0.8f) { // Underload threshold: 0.8x average load
            underloadedBanks.push_back(bankId);
        }
    }
    
    info("Task migration analysis: avg_load=%.2f, overloaded=%zu, underloaded=%zu",
         avgLoad, overloadedBanks.size(), underloadedBanks.size());
    
    // Generate task migration strategy
    for (uint32_t overloadedBank : overloadedBanks) {
        if (underloadedBanks.empty()) break;
        
        uint32_t currentLoad = bankQueueLengths[overloadedBank];
        uint32_t targetLoad = static_cast<uint32_t>(avgLoad);
        
        if (currentLoad > targetLoad) {
            uint32_t tasksToMigrate = (currentLoad - targetLoad) / 2; // Migrate half of the excess tasks
            
            // Select the lightest target bank
            uint32_t targetBank = *std::min_element(underloadedBanks.begin(), underloadedBanks.end(),
                [this](uint32_t a, uint32_t b) {
                    return this->bankQueueLengths[a] < this->bankQueueLengths[b];
                });
            
            // Add task migration command
            bankGroupCommands[overloadedBank].addTaskMigration(overloadedBank, targetBank, tasksToMigrate);
            
            info("Generated task migration: %u tasks from bank %u to bank %u",
                 tasksToMigrate, overloadedBank, targetBank);
            
            // Update load counters (simulate migration effect)
            bankQueueLengths[overloadedBank] -= tasksToMigrate;
            bankQueueLengths[targetBank] += tasksToMigrate;
            
            // If the target bank is no longer underloaded, remove it from the underloaded list
            if (bankQueueLengths[targetBank] >= avgLoad) {
                underloadedBanks.erase(
                    std::remove(underloadedBanks.begin(), underloadedBanks.end(), targetBank),
                    underloadedBanks.end());
            }
        }
    }
    
    info("Task migration generation complete");
}

void BankGroupLoadBalancer::executeTaskMigrations() {
    info("Starting task migration execution");
    
    size_t totalMigrations = 0;
    
    // Traverse all banks' task migration commands
    for (uint32_t bankId : activeBankList) {
        const auto& command = bankGroupCommands[bankId];
        const auto& taskMigrations = command.getTaskMigrations();
        
        if (taskMigrations.empty()) {
            continue;
        }
        
        info("Executing %zu task migrations for bank %u", taskMigrations.size(), bankId);
        
        for (const auto& migration : taskMigrations) {
            migrateTasksBetweenBanks(migration.sourceBankId, migration.targetBankId, migration.taskCount);
            totalMigrations++;
        }
    }
    
    info("Task migration execution complete: %zu migrations processed", totalMigrations);
}

void BankGroupLoadBalancer::migrateTasksBetweenBanks(uint32_t sourceBankId, uint32_t targetBankId, uint32_t taskCount) {
    info("Migrating %u tasks from bank %u to bank %u", taskCount, sourceBankId, targetBankId);
    
    // Select tasks to migrate
    std::vector<Address> tasksToMigrate = selectTasksForMigration(sourceBankId, taskCount);
    
    if (tasksToMigrate.empty()) {
        info("No tasks available for migration from bank %u", sourceBankId);
        return;
    }
    
    // Update local task counters
    if (localDataTaskCount[sourceBankId] >= tasksToMigrate.size()) {
        localDataTaskCount[sourceBankId] -= tasksToMigrate.size();
    }
    localDataTaskCount[targetBankId] += tasksToMigrate.size();
    
    info("Task migration completed: %zu tasks migrated from bank %u to bank %u",
         tasksToMigrate.size(), sourceBankId, targetBankId);
}

std::vector<Address> BankGroupLoadBalancer::selectTasksForMigration(uint32_t sourceBankId, uint32_t taskCount) {
    std::vector<Address> selectedTasks;
    
    // Simplified implementation: select the first taskCount tasks
    // In practice, selection should consider task priority, data locality, etc.
    for (uint32_t i = 0; i < taskCount && selectedTasks.size() < taskCount; i++) {
        Address taskAddr = static_cast<Address>(sourceBankId * 1000 + i); // Simplified address generation
        selectedTasks.push_back(taskAddr);
    }
    
    info("Selected %zu tasks for migration from bank %u", selectedTasks.size(), sourceBankId);
    return selectedTasks;
}

// ===== Phase 4: Validation and Debug Support =====

void BankGroupLoadBalancer::initializeConfigParameters(Config& config) {
    // Load balancing threshold parameters
    loadImbalanceThreshold = config.get<double>("sys.pimBridge.bankGroup.loadImbalanceThreshold", 1.2);
    taskMigrationThreshold = config.get<double>("sys.pimBridge.bankGroup.taskMigrationThreshold", 1.5);
    
    // Task migration parameters
    minTasksForMigration = config.get<uint32_t>("sys.pimBridge.bankGroup.minTasksForMigration", 5);
    maxTasksPerMigration = config.get<uint32_t>("sys.pimBridge.bankGroup.maxTasksPerMigration", 50);
    
    // Feature switches
    enableTaskMigration = config.get<bool>("sys.pimBridge.bankGroup.enableTaskMigration", true);
    enableStorageBankReassignment = config.get<bool>("sys.pimBridge.bankGroup.enableStorageBankReassignment", true);
    
    // Architecture parameters
    storageToComputeRatio = config.get<uint32_t>("sys.pimBridge.bankGroup.storageToComputeRatio", 4);
    
    // Initialize statistics
    totalLoadBalanceOperations = 0;
    totalTaskMigrations = 0;
    totalStorageBankReassignments = 0;
    
    info("BankGroup configuration: loadThreshold=%.2f, taskThreshold=%.2f, storageRatio=%u",
         loadImbalanceThreshold, taskMigrationThreshold, storageToComputeRatio);
}

void BankGroupLoadBalancer::validateBankGroupConfiguration() {
    info("=== Bank Group Configuration Validation ===");
    
    // Validate bank type configuration
    uint32_t activeBankCount = 0;
    uint32_t storageBankCount = 0;
    
    for (uint32_t i = 0; i < totalBanks; i++) {
        if (isActiveBank[i]) activeBankCount++;
        if (isStorageBank[i]) storageBankCount++;
    }
    
    info("Bank Types: total=%u, active=%u, storage=%u", 
         totalBanks, activeBankCount, storageBankCount);
    
    // Validate bank group mappings
    info("Bank Group Mappings:");
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        uint32_t activeBankId = findActiveBankInGroup(groupId);
        const auto& groupBanks = groupToBanks[groupId];
        
        info("  Group %u: active_bank=%u, total_banks=%zu", 
             groupId, activeBankId, groupBanks.size());
        
        // Validate consistency of banks within the group
        for (uint32_t bankId : groupBanks) {
            if (bankToGroup[bankId] != groupId) {
                info("WARNING: Bank %u mapping inconsistency: points to group %u but in group %u",
                     bankId, bankToGroup[bankId], groupId);
            }
        }
    }
    
    // Validate storage-to-compute bank ratio
    float actualRatio = storageBankCount > 0 ? (float)storageBankCount / activeBankCount : 0.0f;
    info("Storage-to-Compute Ratio: configured=%u, actual=%.2f", 
         storageToComputeRatio, actualRatio);
    
    if (abs(actualRatio - storageToComputeRatio) > 1.0f) {
        info("WARNING: Storage-to-compute ratio deviation detected!");
    }
}

void BankGroupLoadBalancer::printLoadBalancingStatistics() {
    info("=== Bank Group Load Balancing Statistics ===");
    
    // Overall statistics
    info("Operations: total=%u, migrations=%u, reassignments=%u, data_ops=%u",
         totalLoadBalanceOperations, totalTaskMigrations, 
         totalStorageBankReassignments);
    
    // Load statistics for each group
    info("Group Load Distribution:");
    float totalLoad = 0.0f;
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        float groupLoad = calculateBankGroupLoad(groupId);
        totalLoad += groupLoad;
        uint32_t activeBankId = findActiveBankInGroup(groupId);
        
        info("  Group %u (Bank %u): load=%.2f, local=%u, managed=%lu",
             groupId, activeBankId, groupLoad,
             activeBankId < localDataTaskCount.size() ? localDataTaskCount[activeBankId] : 0,
             activeBankId < managedDataTaskCount.size() ? managedDataTaskCount[activeBankId].size() : 0);
    }
    
    float avgLoad = numGroups > 0 ? totalLoad / numGroups : 0.0f;
    info("Average Group Load: %.2f", avgLoad);
    
    // Load balancing effect analysis
    float maxLoad = 0.0f;
    float minLoad = std::numeric_limits<float>::max();
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        float groupLoad = calculateBankGroupLoad(groupId);
        maxLoad = std::max(maxLoad, groupLoad);
        minLoad = std::min(minLoad, groupLoad);
    }
    
    float loadImbalance = avgLoad > 0 ? maxLoad / avgLoad : 0.0f;
    info("Load Imbalance Factor: %.2f (max=%.2f, min=%.2f)", 
         loadImbalance, maxLoad, minLoad);
    
    if (loadImbalance > loadImbalanceThreshold) {
        info("WARNING: Load imbalance detected (%.2f > %.2f)", 
             loadImbalance, loadImbalanceThreshold);
    }
}

void BankGroupLoadBalancer::printBankGroupMappings() {
    info("=== Bank Group Mappings ===");
    
    for (uint32_t groupId = 0; groupId < numGroups; groupId++) {
        std::stringstream ss;
        ss << "Group " << groupId << ": ";
        
        uint32_t activeBankId = findActiveBankInGroup(groupId);
        ss << "active=" << activeBankId << ", storage=[";
        
        bool first = true;
        for (uint32_t bankId : groupToBanks[groupId]) {
            if (bankId != activeBankId) {
                if (!first) ss << ",";
                ss << bankId;
                first = false;
            }
        }
        ss << "]";
        
        info("%s", ss.str().c_str());
    }
}

void BankGroupLoadBalancer::validateTaskCounts() {
    info("=== Task Count Validation ===");
    
    uint32_t totalLocalTasks = 0;
    uint32_t totalManagedTasks = 0;
    uint32_t totalStorageTasks = 0;
    
    // Count all tasks
    for (uint32_t bankId : activeBankList) {
        if (bankId < localDataTaskCount.size()) {
            totalLocalTasks += localDataTaskCount[bankId];
        }
        
        if (bankId < managedDataTaskCount.size()) {
            for (const auto& pair : managedDataTaskCount[bankId]) {
                totalManagedTasks += pair.second;
            }
        }
    }
    
    for (uint32_t bankId = 0; bankId < totalBanks; bankId++) {
        if (isStorageBank[bankId] && bankId < storageDataTaskCount.size()) {
            totalStorageTasks += storageDataTaskCount[bankId];
        }
    }
    
    info("Task Distribution: local=%u, managed=%u, storage=%u",
         totalLocalTasks, totalManagedTasks, totalStorageTasks);
    
    // Validate consistency between managed tasks and storage tasks
    if (totalManagedTasks != totalStorageTasks) {
        info("WARNING: Task count inconsistency - managed tasks (%u) != storage tasks (%u)",
             totalManagedTasks, totalStorageTasks);
    }
    
    // Validate task counts for each bank
    for (uint32_t bankId : activeBankList) {
        float groupLoad = calculateBankGroupLoad(bankToGroup[bankId]);
        uint32_t queueLength = bankId < bankQueueLengths.size() ? bankQueueLengths[bankId] : 0;
        
        if (abs(groupLoad - queueLength) > 1.0f) {
            info("WARNING: Bank %u load inconsistency - calculated=%.2f, queue=%u",
                 bankId, groupLoad, queueLength);
        }
    }
}

} // namespace pimbridge

