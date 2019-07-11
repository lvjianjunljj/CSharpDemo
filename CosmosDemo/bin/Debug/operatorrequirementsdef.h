#pragma once

#ifdef DEFINE_OPERATOR_REQUIREMENTS_CONSTANT
#undef DEFINE_OPERATOR_REQUIREMENTS_CONSTANT
#endif

namespace OperatorRequirements
{
    class OperatorRequirementsConstants
    {
    public:
#define DEFINE_OPERATOR_REQUIREMENTS_CONSTANT(name, value) \
        const static unsigned long long name = value;
#include "OperatorRequirements.h"
#undef DEFINE_OPERATOR_REQUIREMENTS_CONSTANT
    };
}
