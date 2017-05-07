//
// Created by nivdror1 on 5/5/17.
//

#ifndef OS_EX3_EXECMAP_H
#define OS_EX3_EXECMAP_H

#include <pthread.h>
#include "MapReduceClient.h"

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
