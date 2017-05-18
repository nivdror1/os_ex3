//
// Created by ido.shachar on 5/4/17.
//
#include <iostream>
#include "MapReduceDerived.h"

/**
 * constructor
 */
MapReduceDerived::MapReduceDerived()
{
}

/**
 * opens the folder and classify the files inside the folder
 * if they contain the search key by sending them through the function emit2
 * @param key the search key
 * @param val the path of the folder
 */
void MapReduceDerived::Map(const k1Base *const key, const v1Base *const val) const
{

	StringContainers * strKey= (StringContainers *)key;
	StringContainers * strVal= (StringContainers *)val;

	DIR * dir;
	struct dirent *file= nullptr;
	//open the directory
	dir= opendir(strVal->getData().c_str());
	if(dir!=NULL){
		//read the directory
		while((file=readdir(dir))!=NULL){
			std::string fileName= file->d_name;
			//check if the search key is contained in the file name
			if(fileName.find(strKey->getData()) != std::string::npos){
				Emit2( new StringContainers(fileName), new IntegerContainers(1));
			}
		}
		if(closedir(dir)==-1){
			std::cerr<<"System call error: could not close the directory"<<std::endl;
			exit(1);
		}
	}else{
		std::cerr<<"System call error: could not open the directory"<<std::endl;
		exit(1);
	}
}

/**
 * send through emit3 the file name and the number of the times it has appeared.
 * @param key the name of the file which contain the search key
 * @param vals a list of int that specify how many files in this specific name were found
 */
void MapReduceDerived::Reduce(const k2Base *const key, const V2_VEC &vals) const
{
	StringContainers * strKey= (StringContainers *)key;
	Emit3(new StringContainers(strKey->getData()), new IntegerContainers(vals.size()));
}