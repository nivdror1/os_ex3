//
// Created by nivdror1 on 5/4/17.
//

#include "StringContainers.h"

/**
 * get the data
 * @return return the data
 */
std::string StringContainers::getData() const
{
    return this->data;
}

/**
 * overloading the operator <
 * @param other k1base instance to be compared
 * @return a boolean according to the result
 */
bool StringContainers::operator<(const k1Base &other) const
{
    StringContainers& key=(StringContainers &)other;
    return this->getData() < key.getData();
}

/**
 * overloading the operator <
 * @param other k2base instance to be compared
 * @return a boolean according to the result
 */
bool StringContainers::operator<(const k2Base &other) const
{
    StringContainers& key=(StringContainers &)other;
    return this->getData() < key.getData();
}

/**
 * overloading the operator <
 * @param other k3base instance to be compared
 * @return a boolean according to the result
 */
bool StringContainers::operator<(const k3Base &other) const
{
    StringContainers& key=(StringContainers &)other;
    return this->getData() < key.getData();
}

