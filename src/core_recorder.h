/** $lic$
 * Copyright (C) 2012-2015 by Massachusetts Institute of Technology
 * Copyright (C) 2010-2013 by The Board of Trustees of Stanford University
 *
 * This file is part of zsim.
 *
 * zsim is free software; you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, version 2.
 *
 * If you use this software in your research, we request that you reference
 * the zsim paper ("ZSim: Fast and Accurate Microarchitectural Simulation of
 * Thousand-Core Systems", Sanchez and Kozyrakis, ISCA-40, June 2013) as the
 * source of the simulator in any publications that use this software, and that
 * you send us a citation of your work.
 *
 * zsim is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef CORE_RECORDER_H_
#define CORE_RECORDER_H_

#include "event_recorder.h"
#include "g_std/g_string.h"

class TimingCoreEvent;

class CoreRecorder {
    private:
        typedef enum {
            HALTED, //Not scheduled, no events left. Initial state. join() --> RUNNING
            RUNNING, //Scheduled. leave() --> DRAINING
            DRAINING //Not scheduled, but events remain. join() --> RUNNING; all events done --> HALTED
        } State;

        State state;

        /* There are 2 clocks:
         *  - phase 1 clock = curCycle and is maintained by the bound phase contention-free core model
         *  - phase 2 clock = curCycle - gapCycles is the zll clock
         *  We maintain gapCycles, and only get curCycle on function calls. Some of those calls also
         *  need to change curCycle, so they just return an updated version that the bound phase model
         *  needs to take. However, **we have no idea about curCycle outside of those calls**.
         *  Defend this invariant with your life or you'll find this horrible to reason about.
         */
        uint64_t gapCycles; //phase 2 clock == curCycle - gapCycles

        //Event bookkeeping
        EventRecorder eventRecorder;
        uint64_t prevRespCycle;
        TimingEvent* prevRespEvent;
        uint64_t lastEventSimulatedStartCycle;
        uint64_t lastEventSimulatedOrigStartCycle;

        //Cycle accounting
        uint64_t totalGapCycles; //does not include gapCycles
        uint64_t totalHaltedCycles; //does not include cycles since last transition to HALTED
        uint64_t lastUnhaltedCycle; //set on transition to HALTED

        uint32_t domain;
        g_string name;

    public:
        CoreRecorder(uint32_t _domain, g_string& _name);

        //Methods called in the bound phase
        uint64_t notifyJoin(uint64_t curCycle); //returns th updated curCycle, if it needs updating
        void notifyLeave(uint64_t curCycle);

        //This better be inlined 100% of the time, it's called on EVERY access
        inline void record(uint64_t startCycle, bool isCritical = true) {
            if (unlikely(eventRecorder.hasRecord())) recordAccess(startCycle, isCritical);
        }

        //Methods called between the bound and weave phases
        uint64_t cSimStart(uint64_t curCycle); //returns updated curCycle
        uint64_t cSimEnd(uint64_t curCycle); //returns updated curCycle

        //Methods called in the weave phase
        inline void reportEventSimulated(TimingCoreEvent* ev);

        //Misc
        inline EventRecorder* getEventRecorder() {return &eventRecorder;}

        //Stats (called fully synchronized)
        uint64_t getUnhaltedCycles(uint64_t curCycle) const;
        uint64_t getContentionCycles() const;
        TimingEvent* getPrevRespEvent() { return this->prevRespEvent; }

    private:
        void recordAccess(uint64_t startCycle, bool isCritical);
};

#endif  // CORE_RECORDER_H_
