#ifndef OSEX3_REDUCEFRAMEWORKEXCEPTION_H
#define OSEX3_REDUCEFRAMEWORKEXCEPTION_H

#include <exception>

#define FAIL_PART_1 "MapReduceFramework Failure: "
#define FAIL_PART_2 " failed."

/*
 * "MapReduceFramework Failure: FUNCTION_NAME failed.", where FUNCTION_NAME is the
 * name of the library call that was failed [e.g. "new"].
 * After each failure the program should be terminated (use exit(1)).
 */

class ReduceFrameworkException : public std::exception {

public:
	ReduceFrameworkException(char* msg);
	
	virtual ~ReduceFrameworkException() throw() {}
	
	virtual const char * getMsg() const throw();

private:
	char *  error;
};

#endif //OSEX3_REDUCEFRAMEWORKEXCEPTION_H