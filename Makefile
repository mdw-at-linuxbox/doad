CURL=/opt/curl
CURL_INC=-I$(CURL)/include
CURL_LIBS=-L$(CURL)/lib -lcurl
JSON_LIBS=-ljson-c
CFLAGS=-g $(CURL_INC)
doad3: doad3.o
	$(CC) -g -o doad3 doad3.o $(CURL_LIBS) $(JSON_LIBS) -lpthread
