//
// Created by nivdror1 on 5/4/17.
//

#include "StringContainers.h"

std::string StringContainers::getData() const
{
    return data;
}

bool StringContainers::operator<(const k1Base &other) const
{
    return this.getData() < (StringContainers)other.getData();
}
