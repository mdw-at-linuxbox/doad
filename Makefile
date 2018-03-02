CURL=/opt/curl
CURL_INC=-I$(CURL)/include
CURL_LIBS=-L$(CURL)/lib -lcurl
CFLAGS=$(CURL_INC)
doad3: doad3.o
	$(CC) -o doad $(CURL_LIBS)
