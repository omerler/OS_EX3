#include <stdlib.h>
#include <stdio.h>
#include "log.h"

bool LogCreated = false;

void logToFile (char *message)
{
	FILE *file;
	
	if (!LogCreated) {
		file = fopen(LOGFILE, "w");
		LogCreated = true;
	}
	else
		try {
			file = fopen(LOGFILE, "a");
		} catch {
			
		}
	
	if (file == NULL) {
		if (LogCreated)
			LogCreated = false;
		return;
	}
	else
	{
		fputs(message, file);
		fclose(file);
	}
	
	if (file)
		fclose(file);
}
