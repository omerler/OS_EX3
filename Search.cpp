
#include <vector>
#include <libltdl/lt_system.h>
#include <cstdio>
#include <stdlib.h>
#include <stdexcept>
#include <iostream>
#include <malloc.h>
#include "MapReduceFramework.h"
#include "ReduceFrameworkException.h"
#include "MapReduceSearch.h"

#define INVALID_INPUT "Usage: <substring to search> <folders, separated by space>"
#define RESERVE "reserve"
#define MALLOC "malloc"

char * stringToSearch;
void * printOutput(OUT_ITEMS_VEC outItemsVec);


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
	
	//Create first inputVector as a vector
	IN_ITEMS_VEC inputVector;
	inputVector.reserve((unsigned long)argc);
	try {
		inputVector.reserve((unsigned long) multiThreadLevel);  // Allocate it's own space
	} catch (const std::length_error &le) { //allocation error
		throw ReduceFrameworkException((char *)RESERVE);
	}
	stringToSearch = (char *) malloc(FILENAME_MAX * sizeof(char));
	if (stringToSearch == NULL) {
		throw ReduceFrameworkException((char *)MALLOC);
	}
	stringToSearch = argv[1];
	
	for (int i = 0; i < numOfLibrariesToSearch; i++) {
		IN_ITEM temp;
		k1BaseSearch k1BaseSearch1 = k1BaseSearch(argv[i]);
		temp.first = &k1BaseSearch1;
		v1BaseSearch v1BaseSearch1 = v1BaseSearch(stringToSearch);
		temp.second = &v1BaseSearch1;
		
		inputVector.push_back(temp);
	}
	
	MapReduceBase mapReduceBase;
	OUT_ITEMS_VEC outItemsVec = RunMapReduceFramework(mapReduceBase, inputVector,
													  multiThreadLevel, false);
	printOutput(outItemsVec);

	return (EXIT_SUCCESS);
}

void * printOutput(OUT_ITEMS_VEC outItemsVec){
	unsigned long outSize = outItemsVec.size();
	for (unsigned long j = 0; j < outSize; j++) {
		OUT_ITEM out_item = outItemsVec.at(j);
		v3BaseSearch* v3BaseSearch1 = (v3BaseSearch*)(out_item.second);
		for (int k = 0; k < v3BaseSearch1->counter; k++) {
			if (k == 0) {
				std::cout << out_item.first;
			} else {
				std::cout << ' ' << out_item.first;
			}
		}
		delete (&out_item.first);
		delete (&out_item.second);
		delete (&out_item);
	}
	delete &outItemsVec;
	
}
