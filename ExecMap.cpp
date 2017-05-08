//
// Created by nivdror1 on 5/5/17.
//

#include "ExecMap.h"

#define CHUNK_SIZE 10

void* ExecMap::mapAll(void*)
{
    // lock and unlock the pTC mutex
    pthread_mutex_lock(&resources.pthreadToContainerMutex);
    pthread_mutex_unlock(&resources.pthreadToContainerMutex);
    int chunkStartingIndex = 0, numberOfIterations = 0;
    IN_ITEM currentItem;
    while (true){
        // lock inputVectorIndex to get the starting index for next chunk to map
        pthread_mutex_lock(&resources.inputVectorIndexMutex);
        chunkStartingIndex = resources.inputVectorIndex;
        resources.inputVectorIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&resources.inputVectorIndexMutex);

        if (chunkStartingIndex >= resources.inputVector.size()){
            break;
        }
        numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
                                      (int)resources.inputVector.size());
        for (int i = chunkStartingIndex; i < numberOfIterations; ++i)
        {
            currentItem = resources.inputVector.at(i);
            resources.mapReduce->Map(currentItem.first, currentItem.second);
        }
    }

}

ExecMap::ExecMap()
{
    int error = pthread_create(&_thread, NULL, mapAll, NULL);
}

Map_Vec* ExecMap::getPastMapVector()
{
    return &_mappingPairs;
}

pthread_t ExecMap::getSelf()
{
    return _thread;
}

void ExecMap::addToMappingVector(std::pair<k2Base *, v2Base *> newPair)
{
    _mappingPairs.push_back(newPair);
}
