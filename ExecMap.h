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

}resources;


typedef std::vector<std::pair<k2Base*, v2Base*>> Map_Vec;


class ExecMap {
private:

    Map_Vec _mappingPairs;

    pthread_t _thread;

    static void* mapAll(void*);

public:

    ExecMap();

	~ExecMap() {}

    Map_Vec* getPastMapVector();

	unsigned long getVectorSize();

    pthread_t getSelf();

    void addToMappingVector(std::pair<k2Base *, v2Base *> newPair);

};


#endif //OS_EX3_EXECMAP_H
