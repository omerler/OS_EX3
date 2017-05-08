
#include <vector>
#include <libltdl/lt_system.h>
#include "MapReduceClient.h"
#include <cstdio>
#include <stdlib.h>
#include <stdexcept>
#include <iostream>
#include <malloc.h>
#include "MapReduceFramework.h"
#include "ReduceFrameworkException.h"

#define INVALID_INPUT "Usage: <substring to search> <folders, separated by space>"
#define RESERVE "reserve"
#define MALLOC "malloc"

char * stringToSearch;

void validateInput(int argc){
	//Check if input is valid.
	if (argc < 2){
		fprintf(stderr,"%s\n", INVALID_INPUT);
		exit(1);
		//no libraries to check, exit.
	}  else if (argc == 2){
		exit(0);
	}
	// else, valid input, return
}

int main(int argc, char* argv[]) {
	validateInput(argc);
	int numOfLibrariesToSearch = argc - 2; //first is SEARCH, second is the string to search
	int multiThreadLevel = numOfLibrariesToSearch; // number of threads for the map level and
	
	//Create first container as a vector
	IN_ITEMS_VEC<IN_ITEM> container;
	try {
		container.reserve((unsigned long) multiThreadLevel);  // Allocate it's own space
	} catch (const std::length_error &le) { //allocation error
		throw ReduceFrameworkException(RESERVE);
	}
	stringToSearch = (char *) malloc(FILENAME_MAX * sizeof(char));
	if (stringToSearch == NULL) {
		throw ReduceFrameworkException(MALLOC);
	}
	stringToSearch = argv[1];
	
	for (int i = 0; i < numOfLibrariesToSearch; i++) {
		IN_ITEM temp;
		temp.first = argv[i];
		temp.second = stringToSearch;
		container.push_back(temp);
	}
	
	MapReduceBase mapReduceBase;
	OUT_ITEMS_VEC outItemsVec = RunMapReduceFramework(&MapReduceBase., container,
													  multiThreadLevel, false);
	
	unsigned long outSize = outItemsVec.size();
	for (unsigned long j = 0; j < outSize; j++) {
		OUT_ITEM out_item = outItemsVec.at(j);
		for (int k = 0; k < out_item.second; k++) {
			if (k == 0) {
				std::cout << out_item.first;
			} else {
				std::cout << ' ' << out_item.first;
			}
		}
		delete (out_item.first);
		delete (out_item.second);
		delete (out_item);
	}
	delete outItemsVec;
	return (EXIT_SUCCESS);
}
