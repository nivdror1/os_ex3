#include "Comparators.h"
#include <pthread.h>
#include <map>
#include <iostream>
#include <stdlib.h>
#include <fstream>
#include "semaphore.h"
#include <time.h>
#include <sys/time.h>
#include "StringContainers.h"
#include "IntegerContainers.h"
#include <algorithm>

#define CHUNK_SIZE 10
#define SECONDS_TO_NANO_SECONDS 1000000000
#define MICRO_SECONDS_TO_NANO_SECONDS 1000


typedef std::vector<std::pair<k2Base*, v2Base*>> MAP_VEC;

typedef std::vector<std::pair<pthread_t,MAP_VEC>> PTC;

typedef std::vector<std::pair<k3Base*, v3Base*>> REDUCE_VEC;

typedef std::vector<std::pair<pthread_t,REDUCE_VEC>> REDUCED_CONTAINERS;

typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;

typedef std::vector<v2Base *> V2_VEC;



/** vector of pairs of thread and his matching after-mapping vector*/
PTC execMapVector;

/** the shuffle thread*/
pthread_t shuffleThread;

/** vector of pairs of thread and his matching after-reducing vector*/
REDUCED_CONTAINERS execReduceVector;

/** the input vector that was given in the runMapReduceFramework*/
MapReduceBase* mapReduce;

/**
 * a semaphore which control the shuffle progressing by up/down the semaphore
 * any time a pair is been added/deleted form the ExecMap containers
 */
sem_t shuffleSemaphore;


/** a vector which contain mutexes of execMap containers*/
std::vector<pthread_mutex_t> mutexVector;

/** the output map of the shuffle process*/
std::map<k2Base*,std::vector<v2Base*>,K2Comp> shuffledMap;

/** final output vector*/
OUT_ITEMS_VEC outputVector;

/** mutex for the initiation of the reduce containers */
pthread_mutex_t reduceVectorMutex;

/** the log file */
std::ofstream myLogFile;

/** a mutex for the log file */
pthread_mutex_t logMutex;


/** a counter of the currently running ExecMap threads*/
int numberOfMappingThreads;




/** a mutex for the counter of the ExecMap currently running*/
pthread_mutex_t numberOfMappingThreadsMutex;

/**
 * a struct of resources for the ExecMap objects
 */
struct MapResources{

    /** a mutex on the pthreadToContainer*/
    pthread_mutex_t pthreadToContainerMutex; // todo maybe change the name

    /** a mutex on the inputVectorIndex*/
    pthread_mutex_t inputVectorIndexMutex;

    /** the input vector that was given in the runMapReduceFramework*/
    IN_ITEMS_VEC inputVector;

    /** the index of current location in the input vector*/
    unsigned int inputVectorIndex;

}MapResources;

struct ShuffleResources{

	/** a vector of indexes that specify where the shuffle is in the passing through the container*/
	std::vector<unsigned int> mapContainerIndex;

}ShuffleResources;

/**
 * a struct of resources for the ExecReduce objects
 */
struct ReduceResources{

	/** a mutex on the shuffledVectorIndex*/
	pthread_mutex_t shuffledVectorIndexMutex;

	/** the index of current location in the shuffled map*/
	unsigned int shuffledMapIndex;

}ReduceResources;


void* mapAll(void*);

void* shuffleAll(void*);

void* reduceAll(void*);

/**
 * convert to nano second
 * @param before the time before the operation
 * @param after the time after the operation
 * @return return the converted time
 */
inline double conversionToNanoSecond(timeval &before, timeval &after){
	return ((after.tv_sec-before.tv_sec)*SECONDS_TO_NANO_SECONDS)
	       +((after.tv_usec- before.tv_usec)*MICRO_SECONDS_TO_NANO_SECONDS);
}

/**
 * get the date and time
 * @return a string representation of the date and time
 */
std::string getDateAndTime(){
	time_t now= time(0);
	struct tm  tstruct;
	char buf[80];
	tstruct = *localtime(&now);
	strftime(buf, sizeof(buf), "%Y:%m:%d:%X", &tstruct);
	std::string s=buf;
	return s;
}

/**
 * create a non specific thread
 * @param thread a reference to a thread to be created
 * @param function the function the thread is supposed to execute
 */
void createThread(pthread_t &thread, void * function(void*)){
	if(pthread_create(&thread, NULL, function, NULL)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_create failed"<<std::endl;
		exit(1);
	}
}

/**
 * join a non specific thread
 * @param thread a thread
 */
void joinThread(pthread_t &thread){
	if (pthread_join(thread, NULL) != 0){
		std::cerr << "MapReduceFramework Failure: pthread_join failed." << std::endl;
		exit(1);
	}
}

/**
 * create a non specific thread
 * @param thread a reference to a thread to be created
 */
void detachThread(pthread_t &thread){
	if(pthread_detach(thread)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_detach failed"<<std::endl;
		exit(1);
	}
}

/**
 * create a non specific mutex
 * @param mutex a mutex
 */
void createMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_init(&mutex,NULL)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_init failed"<<std::endl;
		exit(1);
	}
}

/**
 * lock the mutex
 * @param mutex a mutex
 */
void lockMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_lock(&mutex)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_lock failed"<<std::endl;
		exit(1);
	}
}

/**
 * unlock the mutex
 * @param mutex a mutex
 */
void unlockMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_unlock(&mutex)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_unlock failed"<<std::endl;
		exit(1);
	}
}

/**
 * destroy a mutex
 * @param mutex a  mutex
 */
void destroyMutex(pthread_mutex_t &mutex){
	if(pthread_mutex_destroy(&mutex)!=0){
		std::cerr<<"mapReduceFramework failure: pthread_mutex_destroy failed"<<std::endl;
		exit(1);
	}
}

/**
 * check if getTimeOfDay has failed is so exit
 * @param beforeStatus a return from getTimeOfDay function
 * @param afterStatus a return from getTimeOfDay function
 */
void checkGetTimeOfDayFailure(int &beforeStatus,int &afterStatus){
	if ((beforeStatus | afterStatus) == -1) { //check for a failure in accessing the time
		std::cerr << "MapReduceFramework Failure: getTimeOfDay failed." << std::endl;
		exit(1);
	}
}

/**
 * initiating the map threads
 * In addition initiate the vector which contains the
 * map container and the mutex for each map thread
 * @param numThread number of thread to create
 * */
void mappingThreadsInit(int numThread){
    //spawn the new threads and initiate the vector execMapVector
    for(int i=0;i<numThread;i++){
        // create vector of mapping threads
        MAP_VEC newMappingVector;
        //spawn a new execMap thread
        pthread_t newExecMapThread;
        createThread(newExecMapThread,mapAll);
        execMapVector.push_back(std::make_pair(newExecMapThread, newMappingVector));

    }

}

/**
 * creating the mutexes for the ExecMapThreads
 * @param numThread the number of threads
 */
void creatingExecMapMutexes(int numThread){
	//create the mutex for the inputVectorIndex,ExecMapVector and
	// for the counter of running execMap threads
	createMutex(MapResources.inputVectorIndexMutex);
	createMutex(MapResources.pthreadToContainerMutex);
	createMutex(numberOfMappingThreadsMutex);

	for(unsigned int i=0;i<numThread;i++){
		// create the mutex for the this ExecMap container and append it to the mutexVector
		pthread_mutex_t mapContainerMutex;
		createMutex(mapContainerMutex);
		mutexVector.push_back(mapContainerMutex);
	}
}

/**
 * initiating the shuffle thread and the vector that contains the indexes
 * that point where the shuffle at the passage of the map conainter
 * @param numOfThreads the number of threads
 * @param numOfPairs the nufm of pairs to be shuffled
 */
void shuffleThreadInit(int numOfThreads) {
    // initiating a vector of indexes for every ExecMap container
	for (int i = 0; i < numOfThreads; i++) {
		ShuffleResources.mapContainerIndex.push_back(0);
	}
    //create the shuffle thread
	createThread(shuffleThread, shuffleAll);
}

/**
 * initiating the reduce threads and the vector that contains the thread and
 * his contained
 * @param numThread the number of reduce threads to be created
 */
void reducingThreadsInit(int numThread){
    //creating and locking the mutex so the execReduce threads cannot use Emit3
    // until this thread finish initiating all of the ExecReduce threads and their containers
    createMutex(reduceVectorMutex);
    lockMutex(reduceVectorMutex);

	createMutex(ReduceResources.shuffledVectorIndexMutex);
    //spawn the new threads and initiate the vector execReduceVector
    for(int i=0;i<numThread;i++){
        // create vector of mapping threads
        REDUCE_VEC newReducingVector;
        //spawn a new execReduce thread
        pthread_t newExecReduceThread;
        createThread(newExecReduceThread,reduceAll);
        execReduceVector.push_back(std::make_pair(newExecReduceThread, newReducingVector));

    }
    unlockMutex(reduceVectorMutex);
}

/**
 * write a message to the log file
 * @param message the text to be written
 */
void writingToTheLogFile(std:: string message){
	lockMutex(logMutex);
	try{
		myLogFile<<message;
	}catch (std::ofstream::failure e){
		std::cerr<<"mapReduceFramework failure: write failed"<<std::endl;
		exit(1);
	}
	unlockMutex(logMutex);
}

/**
 * creating the log fle
 * @param numThread the number of threads
 */
void createLogFile(int numThread){

	myLogFile.exceptions(std::ofstream::failbit|std::ofstream::badbit);
	try{
		myLogFile.open(".test"); // todo change the name of the file at the end
		std::string message= "RunMapReduceFramework started with "+std::to_string(numThread)+" threads\n";
		writingToTheLogFile(message);
	}
	catch(std::ofstream::failure e){
		std::cerr<<"mapReduceFramework failure: open|write failed"<<std::endl;
		exit(1);
	}
}

/**
 * close the log file
 */
void closeTheLogFile(){
    try{
        myLogFile.close();
    }catch(std::ofstream::failure e) {
        std::cerr << "MapReduceFramework Failure: close failed." << std::endl;
        exit(1);
    }
}

/**
 * creating the semaphore
 */
void createSemaphore(){
	//initiate the semaphore
	if(sem_init(&shuffleSemaphore,0,0)==-1){
		std::cerr<<"mapReduceFramework failure: sem_init failed"<<std::endl;
		exit(1);
	}
}

/**
 * clear map and reduce vectors and clear shuffledMap
 * and restart the indexes to zero
 */
void restartingResources(){

	MapResources.inputVectorIndex=0;
	ReduceResources.shuffledMapIndex=0;

	ShuffleResources.mapContainerIndex.clear();
	mutexVector.clear();
	execMapVector.clear();
	execReduceVector.clear();
	shuffledMap.clear();
}
/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer.
 * in addition initiating inputVector, numberOfMappingThreads and the mapReduceBase object
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){

	//creating the log file
	createMutex(logMutex);
	createLogFile(numThread);

    // creating the semaphore
	createSemaphore();

	//initiating resources
	MapResources.inputVector = itemsVec;
	mapReduce = &mapReduceBase;
	numberOfMappingThreads = numThread;
	outputVector.clear();

	// creating the mutexes for the ExecMapThreads
	creatingExecMapMutexes(numThread);

	//lock the pthreadToContainer
	lockMutex(MapResources.pthreadToContainerMutex);

    mappingThreadsInit(numThread);
	//
	shuffleThreadInit(numThread);

	//unlock the pthreadToContainer
    unlockMutex(MapResources.pthreadToContainerMutex);
}

/**
 * pass on the execMapVector and then for each thread index
 * pass on the container and delete every k2 and v2 objects
 */
void clearK2V2Vector(){
	//pass on execMapVector
    for (unsigned int threadIndex = 0; threadIndex < execMapVector.size(); ++threadIndex){
	    //get the current container of the execMapThread
        MAP_VEC currentVector = execMapVector.at(threadIndex).second;
	    //Pass on the container and delete k2 and v2 objects
        for (auto itemsIter = currentVector.begin(); itemsIter != currentVector.end() ; ++itemsIter)
        {
            delete (*itemsIter).first;
            delete (*itemsIter).second;
        }
    }
}

/**
 * detach execMapThreads
 * @param numThreads number of threads to detach
 */
void detachExecMapThreads(int numThreads){
	for(unsigned int i=0;i < numThreads;i++){
		detachThread(execMapVector.at(i).first);
		writingToTheLogFile("Thread ExecMap terminated "+ getDateAndTime() +"\n");
	}
}

/**
 * destroy mutexes at the finalizer after the framework has finished working
 */
void terminateMutexesAtTheFinalizer(int numMutexes){
	destroyMutex(reduceVectorMutex);
	destroyMutex(numberOfMappingThreadsMutex);
	destroyMutex(MapResources.pthreadToContainerMutex);
	destroyMutex(ReduceResources.shuffledVectorIndexMutex);

	//destroy for each execMap container it's mutex
	for(unsigned int i=0;i<numMutexes;i++){
		destroyMutex(mutexVector.at(i));
	}
}
/**
 * release all the resourses that allocated during the running of RunMapReduceFramework.
 * @param autoDeleteV2K2 if true, release also V2K2 pairs
 */
void finalizer(bool autoDeleteV2K2, int numThreads){
	//check if there need to delete k2 and v2 objects is so delete them
    if (autoDeleteV2K2){
        clearK2V2Vector();
    }
	//detach the execMapThreads
	detachExecMapThreads(numThreads);

	//clear vectors and zero the index
    restartingResources();

	//destroy the semaphore
	sem_destroy(&shuffleSemaphore);

	//destroy the mutexes
	terminateMutexesAtTheFinalizer(numThreads);

	writingToTheLogFile("RunMapReduceFramework finished\n");

}

/**
 * initialize the RunMapReduceFramework
 * In addition run the ExecMapThreads and the shuffle thread
 * and measure the time that this run takes
 * @param mapReduce a mapReduceBase object that encapsulates the map and reduce functions
 * @param itemsVec the input vector
 * @param multiThreadLevel the number of execMap and ExecReduce threads to create
 */
void runMapAndShuffleThreads(int multiThreadLevel,MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec ){
	struct timeval before, after;
	int beforeStatus, afterStatus;
	//get the starting time
	beforeStatus = gettimeofday(&before, NULL);

	//init the framework
	init(multiThreadLevel,mapReduce,itemsVec);
	//join the shuffle thread
	joinThread(shuffleThread);

	//get the time at the end of the run of map and shuffle
	afterStatus = gettimeofday(&after, NULL);

	//write the time to the log
	checkGetTimeOfDayFailure(beforeStatus,afterStatus);
	double time = conversionToNanoSecond(before,after);
	std::string text="Map and Shuffle took " + std::to_string(time)+ "ns\n";
	writingToTheLogFile(text);

}

/**
 * run the framework i.e run the execMapThreads and the shuffle thread
 * and after they have finished run the execReduceThreads
 * at the end merge the reduce containers and sort the output vector
 * @param mapReduce a mapReduceBase object that encapsulates the map and reduce functions
 * @param itemsVec the input vector
 * @param multiThreadLevel the number of execMap and ExecReduce threads to create
 * @param autoDeleteV2K2 a boolean variable that specify if the framework need to delete the k2 and v2 objects
 * @return the output vector
 */

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
	struct timeval before, after;
	int beforeStatus, afterStatus;
	runMapAndShuffleThreads(multiThreadLevel, mapReduce,itemsVec);

	beforeStatus = gettimeofday(&before, NULL);


    reducingThreadsInit(multiThreadLevel);
	//join the reduce pthreads
	for(unsigned int i=0;i<execReduceVector.size(); i++){
		pthread_join(execReduceVector.at(i).first,NULL);

        //merge the reduce vector and the output vector
        auto curReduceVector=execReduceVector.at(i).second;
        outputVector.reserve( outputVector.size() + curReduceVector.size());
        outputVector.insert( outputVector.end(), curReduceVector.begin(), curReduceVector.end() );

        lockMutex(logMutex);
        myLogFile<<"Thread ExecReduce terminated "+ getDateAndTime() +"\n";
        unlockMutex(logMutex);
	}

    finalizer(autoDeleteV2K2, multiThreadLevel);

	afterStatus = gettimeofday(&after, NULL);
	double time = conversionToNanoSecond(before,after);
	std::string text="Reduce took " + std::to_string(time)+ "ns\n";

	writingToTheLogFile(text);
	destroyMutex(logMutex);
	closeTheLogFile();



	//sort the output
	std::sort(outputVector.begin(),outputVector.end(),K3Comp());
	return outputVector;
}


void Emit2 (k2Base* key, v2Base* value)
{
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execMapVector.size(); ++i)
	{
		if (pthread_equal(execMapVector.at(i).first,currentThreadId))
		{
			lockMutex(mutexVector.at(i));
			execMapVector.at(i).second.push_back(std::make_pair(key, value));
			unlockMutex(mutexVector.at(i));
			break;
		}
	}
	if(sem_post(&shuffleSemaphore)!=0){
		std::cerr<<"mapReduceFramework failure: sem_post failed"<<std::endl;
		exit(1);
	}
}

void Emit3 (k3Base* key, v3Base* value){
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execReduceVector.size(); ++i)
	{
		if (pthread_equal(execReduceVector.at(i).first,currentThreadId))
		{
			auto pair = std::make_pair(key,value);

            execReduceVector.at(i).second.push_back(pair);
			break;
		}
	}
}

void mapCurrentChunk(unsigned int chunkStartingIndex) {
    IN_ITEM currentItem;
    // take the minimum so we don't get out of bounds from input vector
    unsigned int numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
                                  (unsigned int)MapResources.inputVector.size());
    for (unsigned int i = chunkStartingIndex; i < numberOfIterations; ++i)
    {
        // map the current item from input vector
        currentItem = MapResources.inputVector.at(i);
//	    StringContainers *key =(StringContainers*)currentItem.first;
//	    StringContainers *value =(StringContainers*)currentItem.second;
        mapReduce->Map(currentItem.first, currentItem.second);
    }
}


/**
 * mapping function that the thread actually runs, gets chuncks of pairs from input vector and
 * mapping them according to the given map function
 * @return
 */
void* mapAll(void*)
{
	lockMutex(logMutex);
	myLogFile<<"Thread ExecMap created "+ getDateAndTime() +"\n";
	unlockMutex(logMutex);

    // lock and unlock the pTC mutex
	lockMutex(MapResources.pthreadToContainerMutex);
	unlockMutex(MapResources.pthreadToContainerMutex);

    unsigned int currentIndex = 0, currentNumberOfMappingThreads = 0;
    // loop until there are no more pairs to take from input vector
    while (currentIndex < MapResources.inputVector.size()){
        // lock inputVectorIndex to get the starting index for next chunk to map
	    lockMutex(MapResources.inputVectorIndexMutex);
	    currentIndex = MapResources.inputVectorIndex;
        MapResources.inputVectorIndex += CHUNK_SIZE;
        unlockMutex(MapResources.inputVectorIndexMutex);

        mapCurrentChunk(currentIndex);
	    currentIndex += CHUNK_SIZE;
    }
	lockMutex(numberOfMappingThreadsMutex);
	currentNumberOfMappingThreads = --numberOfMappingThreads;
	unlockMutex(numberOfMappingThreadsMutex);
	if (currentNumberOfMappingThreads == 0){
		sem_post(&shuffleSemaphore);
	}
	return 0;
}


/**
 * search the key, if it is in the map append the value to the vector,
 * else add a new pair to the map (key,value)
 * @param key the key on which to search
 * @param value the data that need to append
 */
void searchingAndInsertingData(k2Base* key, v2Base* value){
//    NumWrapper *k= (NumWrapper*)key;
//    NumWrapper *i=(NumWrapper*)value;
	//search the key
	auto search = shuffledMap.find(key);
	//if the key has been found ,append only the value
	if(search != shuffledMap.end()) {
		search->second.push_back(value);
	}
	else{
		// add a new pair
		auto valueVector= std::vector<v2Base*>{value} ;
		shuffledMap.insert(std::make_pair(key, valueVector));

	}
	if (sem_wait(&shuffleSemaphore) != 0) {
		std::cerr << "mapReduceFramework failure: sem_wait failed" << std::endl;
		exit(1);
	}

}

/**
 * shuffle data from a container
 * @param i the index of the execMap containers
}
 * @param pairsShuffled the number of the pairs that had been shuffled
 */
void shufflingDataFromAContainer(unsigned int i){

	//going through the execMapVector
	while(ShuffleResources.mapContainerIndex.at(i) < execMapVector.at(i).second.size()) {
		unsigned int index= ShuffleResources.mapContainerIndex.at(i);

		//lock the mutex of the container
		lockMutex(mutexVector.at(i));

		//get the value from the container
		k2Base *key = execMapVector.at(i).second.at(index).first;
		v2Base *value = execMapVector.at(i).second.at(index).second;

		//unlock the mutex of the container
		unlockMutex(mutexVector.at(i));

		//increase the index value of the specific map container
		ShuffleResources.mapContainerIndex.at(i)+=1;

		// insert the pair into the shuffle container
		searchingAndInsertingData(key,value);

	}
}

/**
 * the function performs the shuffle process
 * this process take every pair from the map containers and insert to
 * the shuffled container which each cell in it contain a key
 * and vector of value that correspond
 * @return do not return anything
 */
void* shuffleAll(void*){

	//std::cout<<"shuffle began"<<std::endl;
	bool mapStillRunning=true;
	lockMutex(logMutex);
	myLogFile<<"Thread shuffle created "+ getDateAndTime() +"\n";
	unlockMutex(logMutex);

	//wait until one of the containers is not empty
	if(sem_wait(&shuffleSemaphore)!=0){
		std::cerr<<"mapReduceFramework failure: sem_wait failed"<<std::endl;
		exit(1);
	}
	//std::cout<<"shuffle "<<std::endl;
	while (mapStillRunning) {
		if(numberOfMappingThreads==0){
			mapStillRunning = false;
		}
		for (unsigned int i = 0; i < execMapVector.size(); i++) {

			// shuffling Data From A specific Container
			shufflingDataFromAContainer(i);
		}
	}
	//std::cout<<"shuffle end and vector size is " << shuffledMap.size() <<std::endl;
	return 0;
}

void reduceCurrentChunck(unsigned int chunkStartingIndex){
    auto iteratingIndex = shuffledMap.begin();
    std::advance(iteratingIndex, chunkStartingIndex);
//	auto c = iteratingIndex->first;
//	auto d = iteratingIndex->second;
//	StringContainers *a= (StringContainers*)iteratingIndex->first;
//	IntegerContainers *b = (IntegerContainers*)iteratingIndex->second.at(0);

	// take the minimum so we don't get out of bounds from shuffled vector
	unsigned int numberOfIterations = std::min(chunkStartingIndex + CHUNK_SIZE,
								  (unsigned int)shuffledMap.size());
	for (unsigned int i = chunkStartingIndex; i < numberOfIterations; ++i)
	{
		mapReduce->Reduce(iteratingIndex->first, iteratingIndex->second);
        ++iteratingIndex;
	}
}

void* reduceAll(void *)
{

	lockMutex(logMutex);
	myLogFile<<"Thread ExecReduce created"+ getDateAndTime() +"\n";
	unlockMutex(logMutex);
    //std::cout<<"reduce begin"<<std::endl;

    lockMutex(reduceVectorMutex);
    unlockMutex(reduceVectorMutex);
	unsigned int currentIndex = 0;

	// loop until there are no more pairs to t
    // +ake from input vector
	while (currentIndex < shuffledMap.size()){
		// lock shuffledVectorIndex to get the starting index for next chunk to map
		lockMutex(ReduceResources.shuffledVectorIndexMutex);
		currentIndex = ReduceResources.shuffledMapIndex;
		ReduceResources.shuffledMapIndex += CHUNK_SIZE;
        //std::cout<<"current reduce index is " << currentIndex <<std::endl;
		unlockMutex(ReduceResources.shuffledVectorIndexMutex);

		reduceCurrentChunck(currentIndex);
		currentIndex += CHUNK_SIZE;

	}
    //std::cout<<"reduce end"<<std::endl;
	return 0;
}