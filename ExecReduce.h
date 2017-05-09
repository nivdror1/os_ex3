//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_EXECREDUCE_H
#define OS_EX3_EXECREDUCE_H

#include <pthread.h>
#include "MapReduceClient.h"


typedef std::pair<k2Base*, std::vector<v2Base*>> SHUFFLED_ITEM;

typedef std::vector<SHUFFLED_ITEM> SHUFFLED_VEC;

typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;

typedef std::vector<std::pair<k3Base*, v3Base*>> Reduce_Vec;

/**
 * a struct of resources for the ExecMap objects
 */
struct ReduceResources{

    /** a mutex on the inputVectorIndexMutex*/
    pthread_mutex_t shuffledVectorIndexMutex;

    /** the input vector that was given in the runMapReduceFramework*/
    SHUFFLED_VEC shuffledVector;

    /** the index of current location in the input vector*/
    int shuffledVectorIndex=0;

    /** an object of mapReduce which contain the map function*/
    MapReduceBase* mapReduce;

}reduceResources;



class ExecReduce {
private:

    /* vector of pairs after the reducing action */
    Reduce_Vec _reducedPairs;

    /* the thread that actually runs */
    pthread_t _thread;

    static void* reduceAll(void*);

public:

    /**
     * a constructor
	 * @return an instance of the class
     */
    ExecReduce();

    /**
	 * d-tor
	 */
    ~ExecReduce() {}

    /**
     * returns pointer to a vector contains all thread mapping pairs
     * @return pointer to a vector contains all thread mapping pairs
     */
    Reduce_Vec* getPastReduceVector();

    /**
     * returns thread id
     * @return thread id
     */
    pthread_t getSelf();

    /**
     * gets new pair of values after reducing action and add it to pairs vector
     * @param newPair
     */
    void addToReducingVector(std::pair<k3Base *, v3Base *> newPair);

};


#endif //OS_EX3_EXECREDUCE_H
