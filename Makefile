CPP= g++ -pthread
CPPFLAGS= -c -g -Wextra -Wvla -Wall -std=c++11 -DNDEBUG
TAR_FILES= Makefile README MapReduceDerived.cpp MapReduceDerived.h MapReduceFramework.cpp 
# All Target
all: MapReduceFramework Search

#Library
MapReduceFramework: MapReduceFramework.o
	ar rcs MapReduceFramework.a MapReduceFramework.o 
	
#Exec Files
Search: Search.o IntegerContainers.o StringContainers.o MapReduceDerived.o MapReduceFramework.o MapReduceFramework.a
	$(CPP) Search.o IntegerContainers.o StringContainers.o MapReduceDerived.o MapReduceFramework.o -L -lMapReduceFramework -o Search


# Object Files
Search.o: Search.cpp MapReduceDerived.h 
	$(CPP) $(CPPFLAGS) Search.cpp -o Search.o
	
MapReduceDerived.o: MapReduceDerived.cpp MapReduceDerived.h 
	$(CPP) $(CPPFLAGS) MapReduceDerived.cpp -o MapReduceDerived.o
	
MapReduceFramework.o: Comparators.h MapReduceFramework.cpp
	$(CPP) $(CPPFLAGS) MapReduceFramework.cpp -o MapReduceFramework.o
	
IntegerContainers.o: IntegerContainers.cpp IntegerContainers.h 
	$(CPP) $(CPPFLAGS) IntegerContainers.cpp -o IntegerContainers.o
	
StringContainers.o: StringContainers.h StringContainers.cpp
	$(CPP) $(CPPFLAGS) StringContainers.cpp -o StringContainers.o

tar:
	tar cvf ex3.tar $(TAR_FILES)
 
# Other Targets
clean:
	-rm -f *.o MapReduceFramework.a Search
