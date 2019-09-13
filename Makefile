all:
	mpic++ -std=c++11 Writer.cpp -ladios2 -o writer
	mpic++ -std=c++11 Reader.cpp -ladios2 -o reader
