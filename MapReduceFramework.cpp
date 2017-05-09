#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include "ExecMap.h"
#include "ExecReduce.h"
#include "semaphore.h"


typedef std::vector<std::pair<pthread_t,Map_Vec*>> PTC;



OUT_ITEMS_VEC outputVector;
std::vector<ExecReduce*> execReduceVector;
pthread_mutex_t *outputVectorMutex;


void* shuffleAll(void*);

struct ShuffleResources{

	/**
	 * a semaphore which control the shuffle progressing by up/down the semaphore
	 * any time a pair is been added/deleted form the ExecMap containers
	 */
	sem_t *shuffleSemaphore;

	/** a vector which contain pointers to execMap objects*/
	std::vector<ExecMap*> execMapVector;

	/** a vector which contain pointers to the mutex of execMap containers*/
	std::vector<pthread_mutex_t*> mutexVector;

	/** the output vector of the shuffle process*/
	std::map<k2Base*,std::vector<v2Base*>> shuffledMap;

	/** the shuffle thread*/
	pthread_t shuffleThread;

	/** the number of pair that need to be shuffled*/
	unsigned long numOfPairs;

	/** a vector of indexes that specify where the shuffle is in the passing through the container*/
	std::vector<unsigned int> mapContainerIndex;

}shuffleResources;


void shuffleThreadInit(int numOfThreads, unsigned long numOfPairs){
	for (int i = 0 ; i < numOfThreads;i++){
		shuffleResources.mapContainerIndex.push_back(0);
	}
	shuffleResources.numOfPairs= numOfPairs;
	int error = pthread_create(&shuffleResources.shuffleThread, NULL, shuffleAll, NULL); //todo error?
}

/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){
	//initiate the semaphore
	int error = sem_init(shuffleResources.shuffleSemaphore,0,1); //todo error

	//update  map resources
	pthread_mutex_init(&mapResources.inputVectorIndexMutex,NULL);
	pthread_mutex_init(&mapResources.pthreadToContainerMutex,NULL);
    mapResources.inputVector=itemsVec;
    mapResources.mapReduce = &mapReduceBase;

	pthread_mutex_init(outputVectorMutex,NULL);

	//lock the pthreadToContainer
	pthread_mutex_lock(&mapResources.pthreadToContainerMutex);
	//spawn the new threads and initiate the vector pthreadToContainer
	for(int i=0;i<numThread;i++){
        // maybe give up on execMapVector and make pTC vector of <thread_self, ExecMap*>
		shuffleResources.execMapVector.push_back(new ExecMap());
		pthread_mutex_t mapContainerMutex;
		pthread_mutex_init(&mapContainerMutex,NULL); //todo check if there are errors of the functions
		shuffleResources.mutexVector.push_back(&mapContainerMutex);

 	}
	//
	shuffleThreadInit(numThread, itemsVec.size());

	//unlock the pthreadToContainer
	pthread_mutex_unlock(&mapResources.pthreadToContainerMutex);


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

	init(multiThreadLevel,mapReduce,itemsVec);
}


void Emit2 (k2Base* key, v2Base* value)
{
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < shuffleResources.execMapVector.size(); ++i)
	{
		if (pthread_equal(shuffleResources.execMapVector[i]->getSelf(),currentThreadId))
		{
			pthread_mutex_lock(shuffleResources.mutexVector.at(i));
			shuffleResources.execMapVector[i]->addToMappingVector(std::make_pair(key, value));
			pthread_mutex_unlock(shuffleResources.mutexVector.at(i));
			break;
		}
	}
	sem_post(shuffleResources.shuffleSemaphore);
}

void Emit3 (k3Base* key, v3Base* value){
	pthread_t currentThreadId = pthread_self();
	for (unsigned int i = 0; i < execReduceVector.size(); ++i)
	{
		if (pthread_equal(execReduceVector[i]->getSelf(),currentThreadId))
		{
			auto pair = std::make_pair(key,value);

			pthread_mutex_lock(outputVectorMutex);
			outputVector.push_back(pair);
			pthread_mutex_unlock(outputVectorMutex);
			break;
		}
	}
}



/**
 * search the key, if it is in the map append the value to the vector,
 * else add a new pair to the map (key,value)
 * @param key the key on which to search
 * @param value the data that need to append
 */
void searchingAndInsertingData(k2Base* key, v2Base* value,unsigned int &pairsShuffled){
	//search the key
	auto search = shuffleResources.shuffledMap.find(key);
	//if the key has been found ,append only the value
	if(search!=shuffleResources.shuffledMap.end()) {
		search->second.push_back(value);
	}
	else{
		// add a new pair
		auto *valueVector= new std::vector<v2Base*>{value} ;
		shuffleResources.shuffledMap.insert(std::make_pair(key, *valueVector));
		//todo do i need to delete the valueVector?

	}
	//increasing the count of the pairs that had been shuffled
	pairsShuffled++;
	sem_wait(shuffleResources.shuffleSemaphore);
}

/**
 * shuffle data from a container
 * @param i the index of the execMap container
 * @param pairsShuffled the number of the pairs that had been shuffled
 */
void shufflingDataFromAContainer(unsigned int i, unsigned int &pairsShuffled){

	//going through the execMapVector
	while(shuffleResources.mapContainerIndex[i] > shuffleResources.execMapVector.at(i)->getVectorSize()) {
		unsigned int index= shuffleResources.mapContainerIndex[i];

		//lock the mutex of the container
		pthread_mutex_lock(shuffleResources.mutexVector[i]);

		//get the value from the container
		k2Base *key = shuffleResources.execMapVector.at(i)->getPastMapVector()->at(index).first;
		v2Base *value = shuffleResources.execMapVector.at(i)->getPastMapVector()->at(index).second;

		//unlock the mutex of the container
		pthread_mutex_unlock(shuffleResources.mutexVector[i]);

		//increase the index value of the specific map container
		shuffleResources.mapContainerIndex[i]+=1;

		// insert the pair into the shuffle container
		searchingAndInsertingData(key,value,pairsShuffled);

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

	unsigned int pairsShuffled = 0;
	//wait until one of the containers is not empty
	sem_wait(shuffleResources.shuffleSemaphore);

	while(pairsShuffled != shuffleResources.numOfPairs){
		for(unsigned int i=0;i<shuffleResources.execMapVector.size();i++){

			// shuffling Data From A specific Container
			shufflingDataFromAContainer(i,pairsShuffled);
		}
	}
}