#include "Comparators.h"
/**
 * a functor whom compares k3 by the operator <
 */

bool K3Comp::operator()(const OUT_ITEM pair1 , const OUT_ITEM pair2 ) const
{
    return *(pair1.first) < *(pair2.first);
}


/**
 * a functor whom compares k2 by the operator <
 */
bool K2Comp::operator()(const k2Base* key , const k2Base* otherKey ) const
{
    return *(key) < *(otherKey);
}
