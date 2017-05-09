//
// Created by nivdror1 on 5/5/17.
//

#include "ExecMap.h"

#define CHUNK_SIZE 10

/**
 * mapping function that the thread actually runs, gets chuncks of pairs from input vector and
 * mapping them according to the given map function
 * @return
 */
void* ExecMap::mapAll(void*)
{
    // lock and unlock the pTC mutex
    pthread_mutex_lock(&mapResources.pthreadToContainerMutex);
    pthread_mutex_unlock(&mapResources.pthreadToContainerMutex);

    int chunkStartingIndex = 0, numberOfIterations = 0;
    IN_ITEM currentItem;
    while (true){
        // lock inputVectorIndex to get the starting index for next chunk to map
        pthread_mutex_lock(&mapResources.inputVectorIndexMutex);
        chunkStartingIndex = mapResources.inputVectorIndex;
        mapResources.inputVectorIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&mapResources.inputVectorIndexMutex);

        if (chunkStartingIndex >= mapResources.inputVector.size())
        {
            // if true, no more pairs to take from input vector, the job is done for current thread
            break;
        }
        // take the minimum so we don't get out of bounds from input vector
        numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
                                      (int)mapResources.inputVector.size());
        for (int i = chunkStartingIndex; i < numberOfIterations; ++i)
        {
            // map the current item from input vector
            currentItem = mapResources.inputVector.at(i);
            mapResources.mapReduce->Map(currentItem.first, currentItem.second);
        }
    }

}

/**
 * a constructor
 * @return an instance of the class
 */
ExecMap::ExecMap()
{
    int error = pthread_create(&_thread, NULL, mapAll, NULL); //todo error?
}

/**
 * returns pointer to a vector contains all thread mapping pairs
 * @return pointer to a vector contains all thread mapping pairs
 */
Map_Vec* ExecMap::getPastMapVector()
{
    return &_mappingPairs;
}

/**
 * returns size of pairs vector
 * @return
 */
unsigned long ExecMap::getVectorSize()
{
    return _mappingPairs.size();
}

/**
 * returns thread id
 * @return thread id
 */
pthread_t ExecMap::getSelf()
{
    return _thread;
}

/**
 * gets new pair of values after mapping and add it to pairs vector
 * @param newPair
 */
void ExecMap::addToMappingVector(std::pair<k2Base *, v2Base *> newPair)
{
    _mappingPairs.push_back(newPair);
}
