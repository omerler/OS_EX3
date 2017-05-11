#include "ReduceFrameworkException.h"

#include <string>


ReduceFrameworkException::ReduceFrameworkException(char * functionName) {
	char * tmp = __CONCAT((char *)FAIL_PART_1, functionName);
	error = __CONCAT(tmp,FAIL_PART_2);
	
}

const char * ReduceFrameworkException::getMsg() const throw()
{
	return error;
}


