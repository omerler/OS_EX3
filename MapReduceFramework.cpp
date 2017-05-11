#include <map>
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <stdlib.h>
#include <libltdl/lt_system.h>
#include "pthread.h"
#include <mutex>
#include <semaphore.h>

/*
 * constants
 */
#define CHUNK 10 //constant that represents the number of couples in a thread


// data structures:
// pThread shuffle - read values from the queue and insert them tham to the map:
//      if exist -> list[len]=1;
//      else new <key2, value2[]>

//shared data structures that need to be protected with mutex:
// pThread_mutex - prevents pThread from running
//  Queue [<key2,value2>]- execMap pushes to it the output of the map part, and the shuffle executing from it.
// Another map of pthread to mutex
// hold a list of mutex for each thread
// The shuffle finds non-empty container, checks if the mutex is blocked and if not- takes it.

/*
 * Data structures
 */

//  int numOfItemsInTheInputVector - the next index in the vector to take chunk from.
unsigned long numOfItemsInTheInputVector;
//IN_ITEMS_VEC& itemsVec- contains all the couple of <k1,v1>
IN_ITEMS_VEC inputCouplesVec;
// couples representing <k2,v2> (output of Map)
typedef std::pair<k2Base*, v2Base*> pair2;
// array of pair2, which is the output of Map
typedef pair2 * k2v2Container; // todo need to be resize to [CHUNK]
// pointer to k2v2container
typedef  k2v2Container* k2v2ContainerP;
//struct representing container of a thread and a mutex for it
struct MutexAndContainerP2{
	mutable pthread_mutex_t mtx;           // mutex for critical section
	k2v2ContainerP k2v2ContainerP1;
	int counter = 0;
};
//  pthreadToContainer - Map that maps pthread_t to its container and mutex
std::map<pthread_t ,MutexAndContainerP2> pthreadToContainerMap;
//pThread execMap_i - receive "chunk" of couples of <key,value>, and Map, and Reduce X
pthread_t * pthreadArray;
//Thread for shuffle
pthread_t shuffleThread;
//Semaphore that represents the number of threads that finished Map
sem_t * semaphore;
//vector that represents the container of the values, after shuffle
typedef std::vector<v2Base *> value2_vec;
//Map that represents the values after shuffle: keys: k2, values: v2 vector
std::map<k2Base* ,value2_vec> map2afterShuffle;



//####################### Omer Variables ##############################################
void * executeWholeMap();
void * ExecMap(void *arg);
void * deallocAll();
void * allocAll();
void * sort(OUT_ITEMS_VEC unsortedVec);
MapReduceBase *innerMapReduce;
OUT_ITEMS_VEC outItemsVec;
std::mutex pthreadToContainerMutex;
std::mutex inputVectorCurIndexMutex;
#include <iostream>
#include <algorithm>
#include "log.h"
#include <vector>
#include <string>
#include <iostream>
#include <time.h>
#include <cstdio>
#include <ctime>
using namespace std;
#define PTHREAD_CREATE "pthread_create"
#define AT "at"
#define ALLOC "Alloc"
#define FAIL_PART_1 "MapReduceFramework Failure: "
#define FAIL_PART_2 " failed."
int numOfThreads;
std::time_t rawTime;
std::tm* timeinfo;
char buffer [80];

//########## Carmel variables ############
#define PTHREAD_JOIN "Join operation failed "
// couples representing <k3,v3> (output of reduce)
typedef std::pair<k3Base*, v3Base*> pair3;
// array of pair3, which is the output of Map
typedef pair3 * k3v3Container; // todo need to be resize to [CHUNK]
// pointer to k3v3container
//struct representing container of a thread and a mutex for it
struct MutexAndContainerP3{
	k2v2ContainerP k3v3ContainerP1;
};
//  pthreadToContainer - Map that maps pthread_t to its container and mutex
std::map<pthread_t ,MutexAndContainerP3> pthreadToContainerAfterReduce;
//pThread execMap_i - receive "chunk" of couples of <key,value>, and Map, and Reduce X
pthread_t * reducePthreadArray;
//Mutex that protects map2aftershuffle mutex
std::mutex map2afterShuffleMutex;



/*
 *
 */
void * executeWholeMap() {
	cout << "executeWholeMap()\n";  // todo delete
	for (int i = 0; i < numOfThreads; i++) {
		int result = pthread_create(&pthreadArray[i], NULL, &ExecMap, NULL); //include allocation
		// inside the function.
		if (result) {
			fprintf(stderr, "%s%s%s",FAIL_PART_1, PTHREAD_CREATE, FAIL_PART_2);
			exit(EXIT_FAILURE);
		} //else, creation succeeded
		
		//log into logFile
		cout <<"Thread ExecMap created " <<strftime(buffer,80,"[%d.%m.%Y %H:%M:%S]",timeinfo) <<
																					  "\n" <<endl;
		MutexAndContainerP2 mutexAndContainerP2;
		mutexAndContainerP2.counter = 0;
		try{
			mutexAndContainerP2.k2v2ContainerP1; //todo validate that alloc is good
		} catch (std::bad_alloc& exc) {
			fprintf(stderr, "%s%s%s",FAIL_PART_1, ALLOC, FAIL_PART_2);
			exit(EXIT_FAILURE);
		}
		pair<pthread_t, MutexAndContainerP2> p = make_pair(pthreadArray[i], mutexAndContainerP2);
		pthreadToContainerMap.insert(p);
		pthread_join(pthreadArray[i], NULL);
	}
}

/*
 * The thread's "body"
 * recive CHUNK couples and call map on each pair
 * lock and then unlock the pthreadToContainerMutex
 */
void * ExecMap(void *arg) {
	cout << "ExecMap()\n";  // todo delete
	pthreadToContainerMutex.try_lock(); //todo- understand why...
	pthreadToContainerMutex.unlock();
	while (true){
		if (!inputVectorCurIndexMutex.try_lock()){ //was already locked by other thread
			continue;
		} else {
			unsigned long curIndex = numOfItemsInTheInputVector;
			numOfItemsInTheInputVector+=CHUNK;
			inputVectorCurIndexMutex.unlock();
			if (curIndex > numOfItemsInTheInputVector){
				break;
			} else {
				IN_ITEMS_VEC in_items_vec;
				in_items_vec.reserve(CHUNK);
				unsigned long j;
				for (j = curIndex;(j < curIndex + CHUNK) && (j < numOfItemsInTheInputVector); j++) {
					try {
						in_items_vec.push_back(inputCouplesVec.at(j));
					} catch(std::out_of_range o){
						fprintf(stderr, "%s%s%s",FAIL_PART_1, AT, FAIL_PART_2);
						exit(EXIT_FAILURE);
					}
				}
				unsigned long i = 0;
				for (i; i < j; i++) {
					innerMapReduce->Map(in_items_vec.at(i).first, //key1
									   in_items_vec.at(i).second); //value1
				}
			}
		}
	}
}


/*added!*/
//typedef std::vector<v2Base *> value2_vec;
//std::map<k2Base* ,value2_vec> map2afterShuffle;


/*
 * execute the actual shuffle- Go over the map2afterShuffle, and check if there is a k2 which equals to the received k2.
 * if so- we will add the value to the values vector and return.
 * If not, we will create new key to the map of k2, and add the value to a new vector
 */
void shuffleOperation(k2Base* k2BaseInput, v2Base* v2BaseInput){
	cout << "shuffleOperation()\n";  // todo delete
	std::map<k2Base* ,value2_vec>:: iterator it;
	
	it = map2afterShuffle.find(k2BaseInput);
	if (it != map2afterShuffle.end()){
		it->second.push_back(v2BaseInput);
		return;
	}
	
	std::vector<v2Base *> vectorValues2;
	vectorValues2.reserve(numOfItemsInTheInputVector);
	vectorValues2.push_back(v2BaseInput);
	map2afterShuffle.insert(std::pair<k2Base*,std::vector<v2Base *>>(k2BaseInput, vectorValues2));
}

//
//void shuffleOperation(k2Base* k2BaseInput, v2Base* v2BaseInput){
//    for(auto const k2Base : map2afterShuffle) {
//        if (k2Base == k2BaseInput){
//            k2Base.second.insert(value2_vec.begin(), v2BaseInput);
//            return;
//        }
//    }
//    std::vector<v2Base *> vectorValues2;
//    value2_vec.insert(value2_vec.begin(), v2BaseInput);
//    map2afterShuffle.insert(std::pair<k2Base*,std::vector<v2Base *>>(k2BaseInput, vectorValues2));
//}


/*
 * ShuffleFunction will be awaked by the semaphore when it is greater then 0.
 * The function will iterate all the threads' containers and check if they are empty.
 *  if they are: continue to next container in the map.
 *  else: unlock the current container -> pull all the data -> lock -> shuffle -> decrease semaphore.
 *  if none of the threads containers have data (it means that the main thread increase the semaphore as a sign that
 *  map mission is done for all of the vector) ->return
 *  todo delete: important- once the semaphore reaches to 0 or below, the thread that called it is blocked (it can be only shuffle) The thread will be unblocked, when the semaphore is 1
 */
void ShuffleFunction() {
	cout << "ShuffleFunction()\n";  // todo delete
	
	bool anyContainerWasFull = false;
	for(auto const &key : pthreadToContainerMap) { //iterate the map
		if (key.second.counter != 0 ) {
			for (int i = 0; i < key.second.counter; i++) {
				pthread_mutex_lock(&key.second.mtx); //lock mutex
				k2v2ContainerP k2v2Containerp = key.second.k2v2ContainerP1; //pull all the data in the container
				pthread_mutex_unlock(&key.second.mtx);//unlock mutex
				shuffleOperation(k2v2Containerp[i]->first, k2v2Containerp[i]->second);
				anyContainerWasFull = true;
				
				sem_wait(semaphore); // decrease the semaphore
			}
		}
	}
	if (!anyContainerWasFull){ //in this case it means that the main function called the shuffle and map is done
		//todo- call reduce
		sem_wait(semaphore); // decrease the semaphore
		return;
	}
	return;
}


/*
 * The function is called for each map action on a single couple.
 * it will use pthread_self() to recognize with pthreadContainer is belong (in pthreadToContainer) ->
 * if it's container is locked (it means that the shuffle was in the middle of pooling the data)
 * 		so the thread will finish it's quantum (in order to let the suffle finish and unlock the
 * 		container).
 * else: it lock the container ->
 *     push(<k2,v2> into the container of the p.thread ->
 *     increase the semaphore ->
 *     unlock current container
 */


void Emit2 (k2Base* k2, v2Base* v2) {
	cout << "Emit2()\n";  // todo delete
	
	pthread_t curThread = pthread_self();
	if (!pthread_mutex_trylock(&pthreadToContainerMap[curThread].mtx)){//was locked
		pthread_join(shuffleThread, NULL); //run EMIT2 only after shuffle runs. todo validate if it's says that
	} else {
		//was not locked
		int size = pthreadToContainerMap[curThread].counter;
		pair2 *pair = new pair2(k2, v2);
		pthreadToContainerMap[curThread].k2v2ContainerP1[size] = pair;
		sem_post(semaphore);
		pthread_mutex_unlock(&pthreadToContainerMap[curThread].mtx);
	}
}

/*
 *
 */
struct less_than_key
{
	inline bool operator() (const OUT_ITEM k3v3pair1, const OUT_ITEM k3v3pair2)
	{
		return (k3v3pair1.first < k3v3pair2.first);
	}
};

void* sort(OUT_ITEMS_VEC unsortedVec){
	cout << "sort()\n";  // todo delete
	std::sort(unsortedVec.begin(), unsortedVec.end(), less_than_key());
}

/*
 * The method allocates memory
 */
void * allocAll(){
	try{
		pthreadArray = new pthread_t[numOfThreads];
		inputCouplesVec.reserve(numOfItemsInTheInputVector);
		//pthreadToContainerMap is allocated with each couple
	}
	catch(std::bad_alloc& exc)
	{
		fprintf(stderr, "%s%s%s",FAIL_PART_1, ALLOC, FAIL_PART_2);
		exit(EXIT_FAILURE);
	}
}

/*
 * The method deallocate all the memory
 */
void * deallocAll(){
	// The threads are deleted inside the "terminated" caused from the thread code end

	cout << "deallocAll()";  // todo delete
	clog <<"omer";
	delete(&pthreadArray);
	delete(&inputCouplesVec);
	delete(&pthreadToContainerMap);
	delete(&semaphore);
	delete(&pthreadToContainerMutex);
	delete(&inputVectorCurIndexMutex);
}



/*
 * will perform as the main thread.
 * lock pthreadToContainerMutex //todo- why it should be protected by mutex
 * create ExecMap threads -> (block immediately after initialization of each thread
 * create pthreadToContainer
 * unblock pthreadToContainerMutex
 * Create pthread by executing chunk of couples from the vector
 * create the container protected by mutex of <k2,v2>
 * give each thread ten couples of <key,value>
 * check if mutex integer = vector.size().
 *  if done -> call shuffle ->
 */
#define TMP "this is another test\n"
OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
									int multiThreadLevel, bool autoDeleteV2K2) {
	//open log file and submit first log
	//freopen( ".MapReduceFramework.log", "w", stdout);
//	std::cout << "path: " << log::path << '\n' ;
//	log::out << "this is another test\n" ;	log::out << "this is a test\n" ; //todo delete
//	log::flush() ;
	logToFile((char *)TMP);
	
	cout << "RunMapReduceFramework started with "<< multiThreadLevel<< " threads\n"<<endl;
	
	//init time parameters
	timeinfo = std::localtime(&rawTime);
	std::time(&rawTime);
		
	cout << "RunMapReduceFramework()\n";  // todo delete
	
	//todo init pthreaArray and alloc;
	numOfItemsInTheInputVector = itemsVec.size();
	numOfThreads = multiThreadLevel;
	allocAll();
	inputCouplesVec = itemsVec;
	innerMapReduce = &mapReduce;
	// todo: shuffleThread = pthread_create()
	executeWholeMap();
	sem_post(semaphore);
	//todo ExecReduce into "output"
	sort(outItemsVec);
	deallocAll();
	return outItemsVec;
}

void* ExecReduce(void* arg){
	while (true){
		if (!map2afterShuffle.size()){
			break;
		}
		else {
			//created vector of pairs sized CHUNK
			std::pair<k2Base *, value2_vec> pair1;
			typedef std::vector<pair> REDUCE_VEC;
			REDUCE_VEC reduce_vec;
			reduce_vec.reserve(CHUNK);
			
			int counter = CHUNK;
			if (!map2afterShuffleMutex.try_lock()) {
				continue;
			}
			for (auto const &key : map2afterShuffle) {
				if (counter) {
					pair1 = make_pair(key.first, key.second);
					reduce_vec.push_back(pair1);
					map2afterShuffle.erase(key.first);
					counter--;
				} else {
					break;
				}
			}
			map2afterShuffleMutex.unlock();
			
			for (unsigned long j = 0; j < reduce_vec.size(); j++) {
				innerMapReduce->Reduce(reduce_vec.at(j).first, reduce_vec.at(j).second);
			}
		}
	}
}



/*
 * The function create reduce threads, creates container and mutex to protect it,
 */
void *executeWholeReduce(){
	for (int i = 0; i < numOfThreads; i++){
		int res = pthread_create(&reducePthreadArray[i], NULL, &ExecReduce, NULL);
		if (res){ //The result is 1 if the creation was failed
			throw ReduceFrameworkException(PTHREAD_CREATE);
		}
		MutexAndContainerP3 mutexAndContainerP3;
		try{
			mutexAndContainerP3.k3v3ContainerP1; //allocate memory
		} catch (std::bad_alloc& exception){
			throw ReduceFrameworkException(ALLOC);
		}
		pair<pthread_t, MutexAndContainerP3> p = make_pair(reducePthreadArray[i], mutexAndContainerP3);
		pthreadToContainerAfterReduce.insert(p);
		int ret = pthread_join(reducePthreadArray[i], NULL);
		if (ret){
			throw ReduceFrameworkException(PTHREAD_JOIN);
		}
	}
}