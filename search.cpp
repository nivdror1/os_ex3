
#include <iostream>
#include <sys/stat.h>
#include "MapReduceDerived.h"
#include <algorithm>

/**
 * print the results
 * @param outputVector a vector which contain the file names and the amount
 */
void printResults(OUT_ITEMS_VEC &outputVector){
	// going over all the vector cells
	for(int j=0;j<outputVector.size();j++){
		StringContainers *result= (StringContainers*)outputVector.at(j).first;
		IntegerContainers *times= (IntegerContainers*)outputVector.at(j).second;

		// printing the file name * the amount of the file
		for (int k=0;k<times->getValue();k++){
			if(j==outputVector.size()-1 && k==times->getValue()-1){
				std::cout<<result->getData();
				break;
			}
			std::cout<<result->getData() <<" ";
		}
	}
}

/**
 * delete resources
 * @param inputVector the vector that contains the search key and path to the folder
 * @param outputVector the vector that being returned from the function runMapReduceFramework
 * @param worker an object of MapReduceDerived
 * @param keyWord the search key
 * */
void deleteResources(IN_ITEMS_VEC &inputVector,
                     OUT_ITEMS_VEC &outputVector,MapReduceDerived *worker,StringContainers *keyWord){
	//delete v3Base and k3Base
	for(int l = 0;l<outputVector.size();l++){
		delete outputVector.at(l).first;
		delete outputVector.at(l).second;
	}
	//delete v2Base
	for(int j=0;j<inputVector.size();j++){
		delete inputVector.at(j).second;
	}
	//delete k1Base
	delete keyWord;
	delete worker;
}

/**
 * check if the path to the folder is indeed lead to a folder if so append to the inputVector
 * @param inputVector the vector that will contains the search key and path to the folder
 * @param keyWord the search key
 * @param paths an array that contain the paths to the supposed to be folders
 * @param numPaths the number of paths
 */
void isDirectory(IN_ITEMS_VEC &inputVector, StringContainers *keyWord,char* paths[],int numPaths){
    struct stat st;
	for(int i = 2; i < numPaths ; i++){
		if(stat(paths[i],&st)==0){
			if(st.st_mode & S_IFDIR!=0){ //check if it a directory
				inputVector.push_back(IN_ITEM(keyWord,
											  new StringContainers(paths[i])));
			}
		}
		//todo i am not sure if there shouldn't be error message
	}
}

int main(int argc,char* argv[]){

	if(argc==1){
		std::cerr<<"Usage: <substring to search> <folders, separated by space>"<<std::endl;
		exit(1);
	}
	MapReduceDerived *worker = new MapReduceDerived();
	IN_ITEMS_VEC inputVector;
	StringContainers *keyWord= new StringContainers(argv[1]);

    //check if the path to the folder is indeed lead to a folder if so append to the inputVector
    isDirectory(inputVector,keyWord,argv,argc);

    //use the MapReduceFramework
	OUT_ITEMS_VEC outputVector = RunMapReduceFramework(*(worker),inputVector,inputVector.size(),true);
    //sort and print the output
	std::sort(outputVector.begin(),outputVector.end());
	printResults(outputVector);
    //delete the resources
	deleteResources(inputVector,outputVector,worker,keyWord);
	return 0;
}

