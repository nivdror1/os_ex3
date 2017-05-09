//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_EXECREDUCE_H
#define OS_EX3_EXECREDUCE_H


typedef std::pair<k2Base*, std::vector<v2Base*>> SHUFFLED_ITEM;

typedef std::vector<SHUFFLED_ITEM> SHUFFLED_VEC;

typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;

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

}ReduceResources;


typedef std::vector<std::pair<k3Base*, v3Base*>> Reduce_Vec;


class ExecReduce {
private:

    Reduce_Vec _reducedPairs;

    pthread_t _thread;

    static void* reduceAll(void*);

public:

    ExecReduce();

    ~ExecReduce() {}

    Reduce_Vec* getPastReduceVector();

    pthread_t getSelf();

    void addToMappingVector(std::pair<k3Base *, v3Base *> newPair);

};


#endif //OS_EX3_EXECREDUCE_H
