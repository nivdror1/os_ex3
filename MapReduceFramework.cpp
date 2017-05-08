#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include "Shuffle.h"
#include "ExecMap.h"
#include "semaphore.h"


typedef std::vector<std::pair<pthread_t,Map_Vec*>> PTC;

std::vector<ExecMap*> execMapVector;
Shuffle * shuffle;
PTC pthreadToContainer;
sem_t semShuffle;
std::vector<pthread_mutex_t*> mutexVector;



/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){
	//initiate the semaphore
	sem_init(&semShuffle,0,0);

	//update resources
	pthread_mutex_init(&resources.inputVectorIndexMutex,NULL);
	pthread_mutex_init(&resources.pthreadToContainerMutex,NULL);
	resources.inputVector=itemsVec;
	resources.mapReduce = &mapReduceBase;

	//lock the pthreadToContainer
	pthread_mutex_lock(&resources.pthreadToContainerMutex);
	//spawn the new threads and initiate the vector pthreadToContainer
	for(int i=0;i<numThread;i++){
        // maybe give up on execMapVector and make pTC vector of <thread_self, ExecMap*>
		execMapVector[i] =  new ExecMap();
		pthread_mutex_t mapContainerMutex;
		pthread_mutex_init(&mapContainerMutex,NULL); //todo check if there are errors of the functions
		mutexVector.push_back(mapContainerMutex);

 	}
	shuffle= new Shuffle(semShuffle,execMapVector, mutexVector, itemsVec.size());

	//unlock the pthreadToContainer
	pthread_mutex_unlock(&resources.pthreadToContainerMutex);


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

	init(multiThreadLevel,mapReduce,itemsVec);
}
