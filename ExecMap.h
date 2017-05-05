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

    mappingFunction _map;

    pthread_t _thread;

    void mapAll();

public:

    ExecMap(int threadId, mappingFunction map);

    Map_Vec* getPastMapVector();

};


#endif //OS_EX3_EXECMAP_H
