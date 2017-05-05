//
// Created by ido.shachar on 5/4/17.
//
#include <iostream>
#include "MapReduceDerived.h"

/**
 * a constructor
 * @return an instance
 */
MapReduceDerived::MapReduceDerived()
{
}

/**
 * a function
 * @param key
 * @param val
 */
void MapReduceDerived::Map(const k1Base *const key, const v1Base *const val) const
{

	StringContainers * strKey= (StringContainers *)key;
	StringContainers * strVal= (StringContainers *)val;

	DIR * dir;
	struct dirent *file;

	dir= opendir(strVal->getData().c_str());
	if(dir!=NULL){
		while(file=readdir(dir)){
			std::string fileName= file->d_name;
			if(fileName.find(strKey->getData())){
				Emit2(new StringContainers(fileName), new IntegerContainers(1));
			}
		}
	}else{
		std::cerr<<"System call error: could not open the directory"<<std::endl;
	}
}

void MapReduceDerived::Reduce(const k2Base *const key, const V2_VEC &vals) const
{
	StringContainers * strKey= (StringContainers *)key;
	Emit3(strKey, new IntegerContainers(vals.size()));
}