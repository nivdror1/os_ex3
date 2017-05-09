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

	/** the number of pair that need to be shuffled*/
	int numOfPairs;

    /** a vector of indexes that specify where the shuffle is in the passing through the container*/
    std::vector<unsigned int> mapContainerIndex;

	/**
	* search the key, if it is in the map append the value to the vector,
	* else add a new pair to the map (key,value)
	* @param key the key on which to search
	* @param value the data that need to append
	*/
	void searchingAndInsertingData(k2Base* key, v2Base* value, unsigned int &pairsShuffled);

	/**
	* shuffle data from a container
	* @param i the index of the execMap container
	* @param pairsShuffled the number of the pairs that had been shuffled
	*/
	void shufflingDataFromAContainer(unsigned int i, unsigned int &pairsShuffled);

public:
	/**
	 * c-tor
	 * */
	Shuffle(unsigned int numInput,int numberOfThreads);

    /**
     * d-tor
     */
    ~Shuffle();

	void* shuffleAll(void*);


};


#endif //OS_EX3_SHUFFLE_H
