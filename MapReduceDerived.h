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
	/**
	 * a constructor
	 */
    MapReduceDerived();

	/**
	 * opens the folder and classify the files inside the folder
	 * if they contain the search key by sending them through the function emit2
	 * @param key the search key
	 * @param val the path of the folder
	 */
	void Map(const k1Base *const key, const v1Base *const val) const override ;

	/**
	 * send through emit3 the file name and the number of the times it has appeared.
	 * @param key the name of the file which contain the search key
	 * @param vals a list of int that specify how many files in this specific name were found
	 */
	void Reduce(const k2Base *const key, const V2_VEC &vals) const override ;
};

#endif //OS_EX3_MAPREDUCEDERIVED_H
