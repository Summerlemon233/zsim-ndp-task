#pragma once
#include <cstdint>

namespace pimbridge {

class CommModule;

class ScatterScheme {
public:
    enum Trigger {
        AfterGather, 
        Interval,
        OnDemand
    };
    Trigger trigger;
    uint32_t packetSize;
    CommModule* commModule;
    ScatterScheme(Trigger _trigger, uint32_t _packetSize)
        : trigger(_trigger), packetSize(_packetSize){}
    virtual bool shouldTrigger() = 0;
    void setCommModule(CommModule* _commModule) { this->commModule = _commModule; }
};

class AfterGatherScatter : public ScatterScheme {
public:
    AfterGatherScatter(uint32_t _packetSize)
        : ScatterScheme(Trigger::AfterGather, _packetSize) {}
    bool shouldTrigger() override;
};

class IntervalScatter : public ScatterScheme {
public: 
    const uint32_t interval;
    IntervalScatter(uint32_t _packetSize, uint32_t _interval)
        : ScatterScheme(Trigger::Interval, _packetSize), interval(_interval) {}
    bool shouldTrigger() override;
};

class OnDemandScatter : public ScatterScheme {
public:
    const uint32_t threshold;
    const uint32_t maxInterval;
    OnDemandScatter(uint32_t _packetSize, uint32_t _threshold, uint32_t _maxInterval)
        : ScatterScheme(Trigger::OnDemand, _packetSize), 
          threshold(_threshold), maxInterval(_maxInterval) {}
    bool shouldTrigger() override;
};

}