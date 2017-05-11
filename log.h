#ifndef OSEX3_LOG_H
#define OSEX3_LOG_H

#define LOGFILE	".MapReduceFramework.log"     // all Log(); messages will be appended to this file

extern bool LogCreated;      // keeps track whether the log file is created or not

void logToFile (char *message);    // logs a message to LOGFILE

#endif //OSEX3_LOG_H
