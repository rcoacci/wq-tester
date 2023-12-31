ROOT=$(realpath $(dir $(lastword $(MAKEFILE_LIST))))
-include $(ROOT)/plat.mk

VPATH = $(ROOT)/src/


INCS=\
-I$(CCTOOLS_PATH)/include \


LIBS_PATH=\
-L$(CCTOOLS_PATH)/lib \

LIBS=\
-lwork_queue \
-ldttools \
-lcrypto \
-lssl \
-lz \

CCFLAGS=$(CCFLAGS_BASE) $(CCFLAGS_OPT)
CXXFLAGS=$(CXXFLAGS_BASE) $(CXXFLAGS_OPT)

CCFLAGS+= -std=gnu11 -ggdb -Wall
CXXFLAGS+= -std=gnu++17 -ggdb -Wall

all: wq-tester wq-work random_input

wq-tester: wq-tester.cpp.o wq_utils.cpp.o
	@echo "GERANDO EXECUTAVEL $@ ----------------------------------------------------------"
	@echo ""
	$(CXX) $(CXXFLAGS) $(CXXLDFLAGS) -o $@ $^ $(LIBS_PATH) $(LIBS)
	@echo ""

wq-work: wq-work.cpp.o
	@echo "GERANDO EXECUTAVEL $@ ----------------------------------------------------------"
	@echo ""
	$(CXX) $(CXXFLAGS) $(CXXLDFLAGS) -o $@ $^ $(LIBS_PATH) $(LIBS)
	@echo ""

random_input: random_input.cpp.o
	@echo "GERANDO EXECUTAVEL $@ ----------------------------------------------------------"
	@echo ""
	$(CXX) $(CXXFLAGS) $(CXXLDFLAGS) -o $@ $^ $(LIBS_PATH) $(LIBS)
	@echo ""

clean:
	-rm wq-tester wq-work random_input *.o

%.c.o: %.c
	@echo "COMPILANDO FONTE $< ------------------------------------------------------------"
	@echo ""
	$(CC) $(CCFLAGS) -c $< -o $@ $(INCS)
	@echo ""

%.cpp.o: %.cpp
	@echo "COMPILANDO FONTE $< ------------------------------------------------------------"
	@echo ""
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCS)
	@echo ""

.SUFFIXES:
