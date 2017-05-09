#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include "Shuffle.h"
#include "ExecMap.h"
#include "semaphore.h"


typedef std::vector<std::pair<pthread_t,Map_Vec*>> PTC;


Shuffle * shuffle;
PTC pthreadToContainer;




/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){
	//initiate the semaphore
	sem_init(shuffleResources.shuffleSemaphore,0,1);

	//update resources
	pthread_mutex_init(&mapResources.inputVectorIndexMutex,NULL);
	pthread_mutex_init(&mapResources.pthreadToContainerMutex,NULL);
	mapResources.inputVector = itemsVec;
	mapResources.mapReduce = &mapReduceBase;

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
	shuffle= new Shuffle(itemsVec.size(),numThread);

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
	for (int i = 0; i < shuffleResources.execMapVector.size(); ++i)
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