//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_SHUFFLE_H
#define OS_EX3_SHUFFLE_H
#include "semaphore.h"
#include "MapReduceClient.h"
#include "ExecMap.h"
#include <pthread.h>
#include <map>

typedef std::pair<k2Base*, std::vector<v2Base*>> SHUFFLED_ITEM;

/**
 * a struct of resources for the shuffle objects
 */
struct ShuffleResources{

	/**
	 * a semaphore which control the shuffle progressing by up/down the semaphore
	 * any time a pair is been added/deleted form the ExecMap containers
	 */
	sem_t *shuffleSemaphore;

	/** a vector which contain pointers to execMap objects*/
	std::vector<ExecMap*> execMapVector;

	/** a vector which contain pointers to the mutex of execMap containers*/
	std::vector<pthread_mutex_t*> mutexVector;

	/** the output vector of the shuffle process*/
	std::map<k2Base*,std::vector<v2Base*>> shuffledMap;

}shuffleResources; //todo think of a shorter name

class Shuffle {

private:
	/** the thread*/
	pthread_t _thread;

	/** the number of pair being shuffled*/
	int numOfPairs;

	 //todo need to send this also the framework

public:
	/**
	 * c-tor
	 * */
	Shuffle(unsigned int numInput);

    /**
     * d-tor
     */
    ~Shuffle();

	void* shuffleAll(void*);


};


#endif //OS_EX3_SHUFFLE_H
