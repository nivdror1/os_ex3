#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include "Shuffle.h"
#include "ExecMap.h"


typedef std::vector<std::pair<pthread_t,Map_Vec*>> PTC;

std::vector<ExecMap*> execMapVector;
Shuffle * shuffle;
PTC pthreadToContainer;
pthread_mutex_t pthreadToContainerMutex;

/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduce){
	//lock the pthreadToContainer
	pthread_mutex_lock(&pthreadToContainerMutex);
	//spawn the new threads and initiate the vector pthreadToContainer
	for(int i=0;i<numThread;i++){
        // maybe give up on execMapVector and make pTC vector of <thread_self, ExecMap*>
		execMapVector[i] =  new ExecMap(i,&mapReduce);
		pthreadToContainer.push_back(std::make_pair(execMapVector[i]->getSelf()
                                                       ,execMapVector[i]->getPastMapVector()));
 	}
	shuffle= new Shuffle();

	//unlock the pthreadToContainer
	pthread_mutex_unlock(&pthreadToContainerMutex);


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

	init(multiThreadLevel,mapReduce);
}
