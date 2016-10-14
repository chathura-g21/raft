CXX = g++ -std=c++11
CPPFLAGS = -g -fpermissive -Wall -I. -I/usr/local/include/thrift -Igen-cpp
LDFLAGS = -lthrift -lpthread
LD = g++

PROGRAMS = server

OBJECTS = WatRaftServer.o WatRaftHandler.o WatRaftState.o WatRaftConfig.o\
	gen-cpp/WatRaft_constants.o gen-cpp/WatRaft.o gen-cpp/WatRaft_types.o

INCFILES = WatRaftHandler.h WatRaftServer.h WatRaftState.h WatRaftConfig.h\
	gen-cpp/WatRaft_constants.h gen-cpp/WatRaft.h gen-cpp/WatRaft_types.h

all: $(PROGRAMS) $(OBJECTS) $(INCFILES)

server: $(OBJECTS)
	$(LD) $^ $(LDFLAGS) -o $@

clean:
	rm -f *.o $(PROGRAMS) *~
