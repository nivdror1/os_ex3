#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include "Shuffle.h"
#include "ExecMap.h"


typedef std::vector<std::pair<pthread_t,Map_Vec*>> PTC;

std::vector<ExecMap*> execMapVector;
Shuffle * shuffle;
PTC pthreadToContainer;



/**
 * initiate the threads of the mapping and shuffling, and also initiate the
 * pthreadToContainer
 * @param numThread the number of threads to be create while mapping
 * @param mapReduce an object that contain the map function
 */
void init(int numThread,MapReduceBase& mapReduceBase,IN_ITEMS_VEC& itemsVec){
	resources.inputVector=itemsVec;
	resources.mapReduce=mapReduceBase;

	//lock the pthreadToContainer
	pthread_mutex_lock(&resources.pthreadToContainerMutex);
	//spawn the new threads and initiate the vector pthreadToContainer
	for(int i=0;i<numThread;i++){
        // maybe give up on execMapVector and make pTC vector of <thread_self, ExecMap*>
		execMapVector[i] =  new ExecMap(&mapReduce,itemsVec,inputVectorIndex);

 	}
	shuffle= new Shuffle();

	//unlock the pthreadToContainer
	pthread_mutex_unlock(&pthreadToContainerMutex);


}

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){

	init(multiThreadLevel,mapReduce);
}
