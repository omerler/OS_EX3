// data structures:
//IN_ITEMS_VEC& itemsVec- contains all the couple of <k1,v1>
//pThread execMap_i - recives "chunk" of couples of <key,value>, and Map, and Reduce X multiThreadLevel
// pThread shuffle - read values from the queue and insert them tham to the map:
//      if exist -> list[len]=1;
//      else new <key2, value2[]>

//shared data structures that need to be protected with mutex:
// pThread_mutex - prevents pThread from running
//  int itemsVecNext - the next index in the vector to take chunk from.
//  Queue [<key2,value2>]- execMap pushes to it the output of the map part, and the shuffle executing from it.
//  pthreadToContainer- Map that maps pthread_t to its container
// Another map of pthread to mutex
// hold a list of mutex for each thread
// The shuffle finds non-empty container, checks if the mutex is blocked and if not- takes it.

/*
// initiate MultylevelThreads memed execMap;
 *
 */

/*
 * will perform as the main thread.
 * lock pthreadToContainer_mutex //todo- why it should be protected by mutex
 * create ExecMap threads -> (block immediately after initialization of each thread
 * create pthreadToContainer
 * unblock pthreadToContainer_mutex
 *
 * Create pthread by executing chunk of couples from the vector
 * create the container protected by mutex of <k2,v2>
 *give each thread ten couples of <key,value>
 * check if mutex integer = vector.size().
 *  if done -> call shuffle ->
 */

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

}


/*
 * The thread's "init"
 * lock and then unlock the pthreadToContainer_mutex
 */
ExecMap(){}


/*
 * ShuffleFunction() will be awaked by the semaphore (when it is greater then 0)
 * it it iterate all the threads' containers and check if they are empty:
 *  if they are: continue to next container in the map.
 *  else: unlock the current container -> pool all the data -> lock -> shuffle -> decrease semaphore.
 *  if none of the threads containers have data (it means that the main thread increase the semaphore as a sign that
 *      map mission is done for all of the vector) ->return
 *
 */
void ShuffleFunction(){

}

/*
 * execute the actual shuffle
 */
void shuffleOperation(){ //todo change arguments

}


/*
 * The function is called for each map action on a a single couple.
 * it will use p_thredSelf() to recognize with pthreadContainer is belong (in pthreadToContainer) ->
 * unlock to the container ->
 * push(<k2,v2> into the container of the p.thread ->
 * increase the semaphore ->
 * lock current container
 */
void Emit2 (k2Base*, v2Base*);







