#include <cstdio>
#include <string>
#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;

std::string kDBPath = "D:\\rocksdb\\dbtest";

int main() {
	DB* db;
	Options options;
	options.create_if_missing = true;

	// open DB
	Status s = DB::Open(options, kDBPath, &db);
	if(s.ok())
		std::cout << "open a db sucessfully .." << std::endl;
	else
	{
		std::cout << "open a db failed .." << std::endl;
		return -1;
	}

	//write and read
	db->Put(WriteOptions(), "key1", "value");
	std::string value;
	db->Get(ReadOptions(), "key1", &value);
	std::cout << "read key1:" << value << std::endl;

	//batch op
	{
		WriteBatch batch;
		batch.Delete("key1");
		batch.Put("key2", value);
		s = db->Write(WriteOptions(), &batch);
	}

	//check batch op result
	s = db->Get(ReadOptions(), "key1", &value);
	if (s.IsNotFound()) 
	{
		std::cout << "key1 is already delete .."<<std::endl;
	}
	db->Get(ReadOptions(), "key2", &value);
	if (value == "value") 
	{
		std::cout << "key1 is read sucessfully £¬value=" << value << std::endl;
	}

	//colse db
	delete db;

	return 0;
}