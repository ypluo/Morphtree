// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
// Modified by Luo Yongping ypluo18@qq.com

#ifndef __EPOCH_H__
#define __EPOCH_H__

#include <vector>
#include <atomic>
#include <cassert>

#include "node.h"
#include "tbb/combinable.h"
#include "tbb/enumerable_thread_specific.h"

namespace morphtree {

using MyReclaimEle = ReclaimEle;
static inline void ReclaimOneNode(MyReclaimEle & ele) {
	BaseNode * node = (BaseNode *)ele.first;

	if(ele.second == false)
		node->DeleteNode(); // should also reclaim its data
	else 
		delete (char *)node; // reclaim its header only
}

// Epoch based Memory Reclaim
class ThreadSpecificEpochBasedReclamationInformation {
	std::array<std::vector<MyReclaimEle>, 3> mFreeLists;
	std::atomic<uint32_t> mLocalEpoch;
	uint32_t mPreviouslyAccessedEpoch;
	bool mThreadWantsToAdvance;

public:
	ThreadSpecificEpochBasedReclamationInformation()
		: mFreeLists(), mLocalEpoch(3), mPreviouslyAccessedEpoch(3),
			mThreadWantsToAdvance(false) {}

	ThreadSpecificEpochBasedReclamationInformation(
		ThreadSpecificEpochBasedReclamationInformation const &other) = delete;

	ThreadSpecificEpochBasedReclamationInformation(
		ThreadSpecificEpochBasedReclamationInformation &&other) = delete;

	~ThreadSpecificEpochBasedReclamationInformation() {
		for (uint32_t i = 0; i < 3; ++i) {
			freeForEpoch(i);
		}
	}

	void scheduleForDeletion(MyReclaimEle &ele) {
		assert(mLocalEpoch != 3);
		std::vector<MyReclaimEle> &currentFreeList = mFreeLists[mLocalEpoch];
		currentFreeList.emplace_back(ele);
		mThreadWantsToAdvance = (currentFreeList.size() % 64u) == 0;
	}

	uint32_t getLocalEpoch() const {
		return mLocalEpoch.load(std::memory_order_acquire);
	}

	void enter(uint32_t newEpoch) {
		assert(mLocalEpoch == 3);
		if (mPreviouslyAccessedEpoch != newEpoch) {
			freeForEpoch(newEpoch);
			mThreadWantsToAdvance = false;
			mPreviouslyAccessedEpoch = newEpoch;
		}
		mLocalEpoch.store(newEpoch, std::memory_order_release);
	}

	void leave() {
		mLocalEpoch.store(3, std::memory_order_release); 
	}

	bool doesThreadWantToAdvanceEpoch() { return (mThreadWantsToAdvance); }

private:
	void freeForEpoch(uint32_t epoch) {
		std::vector<MyReclaimEle> &previousFreeList = mFreeLists[epoch];
		for (MyReclaimEle & ele : previousFreeList) {
			ReclaimOneNode(ele);
		}
		previousFreeList.resize(0u);
	}
};

class EpochBasedMemoryReclamationStrategy {
public:
	uint32_t NEXT_EPOCH[3] = {1, 2, 0};
	uint32_t PREVIOUS_EPOCH[3] = {2, 0, 1};

	std::atomic<uint32_t> mCurrentEpoch;
	tbb::enumerable_thread_specific<
				ThreadSpecificEpochBasedReclamationInformation, 
			tbb::cache_aligned_allocator<ThreadSpecificEpochBasedReclamationInformation>, 
			tbb::ets_key_per_instance> mThreadSpecificInformations;

private:
	EpochBasedMemoryReclamationStrategy()
		: mCurrentEpoch(0), mThreadSpecificInformations() {}

public:
	static EpochBasedMemoryReclamationStrategy *getInstance() {
		static EpochBasedMemoryReclamationStrategy instance;
		return &instance;
	}

	void enterCriticalSection() {
		ThreadSpecificEpochBasedReclamationInformation &currentMemoryInformation =
			mThreadSpecificInformations.local();
		uint32_t currentEpoch = mCurrentEpoch.load(std::memory_order_acquire);
		currentMemoryInformation.enter(currentEpoch);
		if (currentMemoryInformation.doesThreadWantToAdvanceEpoch() &&
			canAdvance(currentEpoch)) {
			mCurrentEpoch.compare_exchange_strong(currentEpoch,
												NEXT_EPOCH[currentEpoch]);
		}
	}

	bool canAdvance(uint32_t currentEpoch) {
		uint32_t previousEpoch = PREVIOUS_EPOCH[currentEpoch];
		return !std::any_of(
			mThreadSpecificInformations.begin(),
			mThreadSpecificInformations.end(),
			[previousEpoch](ThreadSpecificEpochBasedReclamationInformation const
								&threadInformation) {
			return (threadInformation.getLocalEpoch() == previousEpoch);
			});
	}

	void leaveCriticialSection() {
		ThreadSpecificEpochBasedReclamationInformation &currentMemoryInformation =
			mThreadSpecificInformations.local();
		currentMemoryInformation.leave();
	}

	void  scheduleForDeletion(MyReclaimEle ele) {
		mThreadSpecificInformations.local().scheduleForDeletion(ele);
	}

	void clearAll() {
		mThreadSpecificInformations.clear();
	}
};


class EpochGuard {
	EpochBasedMemoryReclamationStrategy *instance;

public:
	EpochGuard() {
		instance = EpochBasedMemoryReclamationStrategy::getInstance();
		instance->enterCriticalSection();
	}

	~EpochGuard() { instance->leaveCriticialSection(); }
};

extern EpochBasedMemoryReclamationStrategy *ebr;

} // namespace morphtree

#endif //__EPOCH_H__