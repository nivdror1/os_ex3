//
// Created by niv on 5/16/2017.
//

#ifndef OS_EX3_COMPARATORS_H
#define OS_EX3_COMPARATORS_H

#include "MapReduceClient.h"
#include "MapReduceFramework.h"

/**
 * a functor whom compares k3 by the operator <
 */
class K3Comp{
public:
    bool operator()(const OUT_ITEM pair1 , const OUT_ITEM pair2 ) const;
};

/**
 * a functor whom compares k2 by the operator <
 */
class K2Comp{
public:
    bool operator()(const k2Base* key , const k2Base* otherKey ) const;
};
#endif //OS_EX3_COMPARATORS_H
