//
// Created by nivdror1 on 5/4/17.
//

#ifndef OS_EX3_STRINGCONTAINERS_H
#define OS_EX3_STRINGCONTAINERS_H

#include "MapReduceClient.h"
#include <string>
class StringContainers :public k1Base, public k2Base,  public k3Base, public v1Base{
private:
	std::string data;

public:
	/**
	 * a constructor
	 * @param newValue the value
	 * @return an instance of the class
	 */
	StringContainers(std::string data): data(data){}

	/**
	 * d-tor
	 */
	virtual ~StringContainers() {}

	/**
	 * get the data
	 * @return return the data
	 */
	std::string getData() const ;

	/**
	 * overloading the operator <
	 * @param other k1base instance to be compared
	 * @return a boolean according to the result
	 */
	bool operator<(const k1Base &other) const override ;

	/**
	 * overloading the operator <
	 * @param other k2base instance to be compared
	 * @return a boolean according to the result
	 */
	bool operator<(const k2Base &other) const override ;

	/**
	 * overloading the operator <
	 * @param other k3base instance to be compared
	 * @return a boolean according to the result
	 */
	bool operator<(const k3Base &other) const override ;

};


#endif //OS_EX3_STRINGCONTAINERS_H
