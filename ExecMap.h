//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_EXECMAP_H
#define OS_EX3_EXECMAP_H

#include <pthread.h>
#include "MapReduceClient.h"

typedef std::pair<k1Base*, v1Base*> IN_ITEM;

typedef std::vector<IN_ITEM> IN_ITEMS_VEC;

/**
 * a struct of resources for the ExecMap objects
 */
struct MapResources{
	/** a mutex on the pthreadToCotnainer*/
	pthread_mutex_t pthreadToContainerMutex;

	/** a mutex on the inputVectorIndexMutex*/
	pthread_mutex_t inputVectorIndexMutex;

	/** the input vector that was given in the runMapReduceFramework*/
	IN_ITEMS_VEC inputVector;

	/** the index of current location in the input vector*/
	int inputVectorIndex=0;

	/** an object of mapReduce which contain the map function*/
	MapReduceBase* mapReduce;

}mapResources;


typedef std::vector<std::pair<k2Base*, v2Base*>> Map_Vec;


class ExecMap {
private:

	/* vector of pairs after the mapping action */
    Map_Vec _mappingPairs;

	/* the thread that actually runs */
    pthread_t _thread;

	/**
	 * mapping function that the thread actually runs, gets chuncks of pairs from input vector and
	 * mapping them according to the given map function
	 * @return
	 */
    static void* mapAll(void*);

public:

    /**
     * a constructor
	 * @return an instance of the class
     */
    ExecMap();

    /**
	 * d-tor
	 */
	~ExecMap() {}

    /**
     * returns pointer to a vector contains all thread mapping pairs
     * @return pointer to a vector contains all thread mapping pairs
     */
    Map_Vec* getPastMapVector();

    /**
     * returns size of pairs vector
     * @return
     */
	unsigned long getVectorSize();

    /**
     * returns thread id
     * @return thread id
     */
    pthread_t getSelf();

    /**
     * gets new pair of values after mapping and add it to pairs vector
     * @param newPair
     */
    void addToMappingVector(std::pair<k2Base *, v2Base *> newPair);

};


#endif //OS_EX3_EXECMAP_H
