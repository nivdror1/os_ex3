//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_EXECMAP_H
#define OS_EX3_EXECMAP_H

#include <pthread.h>
#include "MapReduceClient.h"

/**
 * a struct of resources for the ExecMap objects
 */
struct Resources{
	/** a mutex on the pthreadToCotnainer*/
	pthread_mutex_t pthreadToContainerMutex;

	/** a mutex on the inputVectorIndexMutex*/
	pthread_mutex_t inputVectorIndexMutex;

	/** the input vector that was given in the runMapReduceFramework*/
	IN_ITEMS_VEC inputVector;

	/** the index of current location in the input vector*/
	int inputVectorIndex=0;

	/** an object of mapReduce which contain the map function*/
	MapReduceBase mapReduce;

}resources;

typedef void (*mappingFunction)(const k1Base *const, const v1Base *const );

typedef std::vector<std::pair<k2Base*, v2Base*>> Map_Vec;


class ExecMap {
private:

    int _threadId;

    Map_Vec _mappingPairs;

    MapReduceBase* _map;

    pthread_t _thread;

    static void* mapAll(void*);

public:

    ExecMap(int threadId, MapReduceBase* map);

    Map_Vec* getPastMapVector();

    pthread_t getSelf();

};


#endif //OS_EX3_EXECMAP_H
