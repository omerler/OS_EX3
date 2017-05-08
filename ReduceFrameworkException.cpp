#include "ReduceFrameworkException.h"

#include <string>


ReduceFrameworkException::ReduceFrameworkException(std::string functionName) {
	error = FAIL_PART_1 + functionName + FAIL_PART_2;
}

const std::string ReduceFrameworkException::getMsg() const throw()
{
	return error;
}


