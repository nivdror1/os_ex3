//
// Created by nivdror1 on 5/5/17.
//

#include "ExecReduce.h"

void* ExecReduce::reduceAll(void *)
{

}

ExecReduce::ExecReduce()
{
    int error = pthread_create(&_thread, NULL, reduceAll, NULL); //todo error?
}

Reduce_Vec* ExecReduce::getPastReduceVector()
{
    return &_reducedPairs;
}

pthread_t ExecReduce::getSelf()
{
    return _thread;
}

void ExecReduce::addToReducingVector(std::pair < k3Base * , v3Base * > newPair)
{
    _reducedPairs.push_back(newPair);
}