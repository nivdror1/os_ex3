//
// Created by ido.shachar on 5/4/17.
//

#include "MapReduceDerived.h"

MapReduceDerived::MapReduceDerived()
{
}

void MapReduceBase::Map(const k1Base *const key, const v1Base *const val) const
{
	StringContainers * strKey= (StringContainers *)key;
	StringContainers * strVal= (StringContainers *)val;

	fs::directory_iterator end_iter;
	for(fs::directory_iterator iter(strVal->getData());
	    iter!=end_iter;++iter){
		std::string fileName = iter->path().filename().string();
		if(fileName.find(strKey->getData())){
			Emit2(new StringContainers(fileName), new IntegerContainers(1));
		}
	}
}

void MapReduceBase::Reduce(const k2Base *const key, const V2_VEC &vals) const
{

}