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
	ReduceFrameworkException(std::string msg);
	
	~ReduceFrameworkException() throw() {}
	
	virtual const std::string getMsg() const throw();

private:
	std::string error;
};

#endif //OSEX3_REDUCEFRAMEWORKEXCEPTION_H