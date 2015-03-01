HEADERS := $(wildcard *.h)

CXX = g++
CFLAGS = -g -Wall

LIB_OBJS= codec.o gotable.o proto.o

all: libgotable.a

libgotable.a:$(LIB_OBJS) $(HEADERS)
	ar -rs $@ $(LIB_OBJS)

example: example.o libgotable.a
	$(CXX) $(CFLAGS) -Wno-strict-aliasing -o $@ $^ libgotable.a

clean:
	rm -f *.o *.a example

.cpp.o:
	$(CXX) $(CFLAGS) -c $^
.c.o:
	$(CXX) $(CFLAGS) -c $^
