//
// Created by nivdror1 on 5/4/17.
//

#ifndef OS_EX3_INTEGERCONTAINERS_H
#define OS_EX3_INTEGERCONTAINERS_H

#include "MapReduceClient.h"

class IntegerContainers : public v2Base, public v3Base{
private:
	int value;

public:
	/**
	 * a constructor
	 * @param newValue the value
	 * @return an instance of the class
	 */
	IntegerContainers(int newValue);

	/**
	 * d-tor
	 */
	virtual ~IntegerContainers();
	/**
	 * get the value
	 * @return return the value
	 */
	int getValue() const;

	/**
	 * set the value
	 */
	void setValue(int newVal);
};


#endif //OS_EX3_INTEGERCONTAINERS_H
