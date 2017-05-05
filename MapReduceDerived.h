//
// Created by ido.shachar on 5/4/17.
//

#ifndef OS_EX3_MAPREDUCEDERIVED_H
#define OS_EX3_MAPREDUCEDERIVED_H

#include "MapReduceFramework.h"
#include "IntegerContainers.h"
#include "StringContainers.h"
#include "dirent.h"



class MapReduceDerived: public MapReduceBase {

public:
    MapReduceDerived();

	void Map(const k1Base *const key, const v1Base *const val) const override ;

	void Reduce(const k2Base *const key, const V2_VEC &vals) const override ;
};

#endif //OS_EX3_MAPREDUCEDERIVED_H
