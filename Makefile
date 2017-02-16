TARGET = lamposk
CC = gcc
CFLAGS = -g -Wall
LIBS = -pthread -lconfig

.PRECIOUS: $(TARGET) $(OBJECTS)
OBJECTS = $(patsubst %.c,%.o,$(wildcard *.c))
HEADERS = $(wildcard *.h)

.PHONY: default all clean
default: $(TARGET)
all: default
clean:
	-rm -f *.o $(TARGET)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -Wall $(LIBS) -o $@