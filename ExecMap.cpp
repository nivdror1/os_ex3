//
// Created by nivdror1 on 5/5/17.
//

#include "ExecMap.h"

void* ExecMap::mapAll(void*)
{
    // lock and unlock the pTC mutex
}

ExecMap::ExecMap(int threadId, MapReduceBase* map): _threadId(threadId), _map(map)
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

