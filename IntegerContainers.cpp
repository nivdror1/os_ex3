

#include "IntegerContainers.h"


/**
 * a constructor
 * @param newValue the value
 * @return an instance of the class
 */
IntegerContainers::IntegerContainers(int newValue):
		value(newValue){};

/**
 * d-tor
 */
~IntegerContainers(){
	this->value=0;
}

/**
 * get the value
 * @return return the value
 */
int IntegerContainers::getValue() const{
	return this->value;
}

/**
 * set the value
 */
void IntegerContainers::setValue(int newVal){
	this->value=newVal;
}