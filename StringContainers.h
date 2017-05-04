//
// Created by nivdror1 on 5/4/17.
//

#ifndef OS_EX3_STRINGCONTAINERS_H
#define OS_EX3_STRINGCONTAINERS_H

#include "MapReduceClient.h"
#include <string>
class StringContainers :public k1Base,public k2Base, public k3Base, public v1Base{
private:
	String data;

public:
	StringContainers(std::string data): data(data){}

	virtual ~StringContainers() {}

	std::string getData() const ;

};


#endif //OS_EX3_STRINGCONTAINERS_H
