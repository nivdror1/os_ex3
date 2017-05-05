//
// Created by nivdror1 on 5/5/17.
//

#include "ExecMap.h"

void ExecMap::mapAll()
{
    // lock and unlock the pTC mute
}

ExecMap::ExecMap(int threadId, mappingFunction map): _threadId(threadId), _map(map)
{
    int error = pthread_create(&_thread, NULL, mapAll, NULL);
}

Map_Vec* ExecMap::getPastMapVector()
{
    return &_mappingPairs;
}