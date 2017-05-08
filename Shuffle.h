//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_SHUFFLE_H
#define OS_EX3_SHUFFLE_H
#include "semaphore.h"
#include "MapReduceClient.h"
#include "ExecMap.h"
#include <pthread.h>

typedef std::pair<k2Base*, std::vector<v2Base*>> SHUFFLED_ITEM;

class Shuffle {

private:
	/** the thread*/
	pthread_t _thread;

	sem_t *shuffleSemaphore;

	std::vector<ExecMap*> execMapVector;

	std::vector<pthread_mutex_t*> mutexVector;

	int numOfPairs;

	/** the output vector of the shuffle process*/
	std::vector<SHUFFLED_ITEM> shuffledVector; //todo need to send this also the framework

public:
	/**
	 * c-tor
	 * */
	Shuffle(sem_t &semShuffle,std::vector<ExecMap*> &execMapVector,
	        std::vector<pthread_mutex_t> mutexVector, unsigned int numInput);


	void* shuffleAll(void*);


};


#endif //OS_EX3_SHUFFLE_H
