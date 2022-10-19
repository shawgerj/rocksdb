// John Shawger
// Primitive version of SharedLogDB - RocksDB storing values in a shared log
// Inspired by WiscKey value log

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <cstdio>
#include <string>
#include <iostream>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "../util/random.h"

using namespace rocksdb;

std::string raftDBPath = "/tmp/rocksdb_raftdb";
std::string kvDBPath = "/tmp/rocksdb_kvdb";
Options globalDBOptions;

class KVPair {
public:
    KVPair(Slice& key, Slice& value)
        : _key(key),
          _value(value) {}

    Slice GetKey() { return _key; }
    Slice GetValue() { return _value; }

private:
    Slice _key;
    Slice _value;
};

// Helper for quickly generating random data.
// Taken from tools/db_bench_tool.cc
class RandomGenerator {
private:
    std::string data_;
    unsigned int pos_;

public:
    RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < (unsigned)std::max(1048576, 32*1024)) {
            // Add a short fragment that is as compressible as specified
            // by FLAGS_compression_ratio.
            CompressibleString(&rnd, 0.5, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    Slice Generate(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }

    Slice GenerateWithTTL(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }

    // RandomString() and CompressibleString() shamelessly copied from
    // ../test_util/testutil.cc into this class. Just for generating
    // random strings of data
    Slice RandomString(Random* rnd, int len, std::string* dst) {
        dst->resize(len);
        for (int i = 0; i < len; i++) {
            (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
        }
        return Slice(*dst);
    }

    Slice CompressibleString(Random* rnd, double compressed_fraction,
                             int len, std::string* dst) {
        int raw = static_cast<int>(len * compressed_fraction);
        if (raw < 1) raw = 1;
        std::string raw_data;
        RandomString(rnd, raw, &raw_data);

        // Duplicate the random data until we have filled "len" bytes
        dst->clear();
        while (dst->size() < (unsigned int)len) {
            dst->append(raw_data);
        }
        dst->resize(len);
        return Slice(*dst);
    }
};


// these methods are thin wrappers over standard rocksdb methods. 
class SharedLogDB {
public:
    SharedLogDB(const std::string& path, const std::string& logfile,
                const Options& options);
    
    // append to logfile, put to db with WAL disabled
    Status Put(const WriteOptions& options, const Slice& key,
                      const Slice& value, off_t* offset);

    // put to db with WAL disabled, do not write to logfile
    Status PutNoLog(const WriteOptions& options, const Slice& key,
                           const Slice& value);

    // get from DB
    // parse key, get from logfile
    Status Get(const ReadOptions& options, const Slice& key,
                      std::string* value);

    ~SharedLogDB();

private:
    DB* _db;
    std::string _name;
    std::string _logfile_name;
    std::string _path;
    int _logfile;
};

SharedLogDB::SharedLogDB(const std::string& path, const std::string& logfile,
    const Options& options) {
    // options settings copied from example program
    // create db
    Status s = DB::Open(options, path, &_db);
    assert(s.ok());

    // create logfile
    _logfile_name = std::string(logfile);
    _logfile = open(_logfile_name.c_str(), O_RDWR| O_CREAT | O_APPEND, S_IRWXU);

    if (_logfile < 0) {
        std::cout << "Error opening logfile" << _logfile_name << std::endl;
        exit(1);
    }
}

Status SharedLogDB::Put(const WriteOptions& options, const Slice& key,
                        const Slice& value, off_t* offset) {
    size_t ksize = key.size();
    size_t vsize = value.size();

    off_t log_offset = lseek(_logfile, 0, SEEK_CUR);
    if (offset < 0) {
        std::cout << "Error seeking in logfile" << _logfile_name << std::endl;
    }
    *offset = log_offset;

    if (write(_logfile, &ksize, sizeof(size_t)) < 0)
        std::cout << strerror(errno) << std::endl;
    if (write(_logfile, &vsize, sizeof(size_t)) != sizeof(size_t))
        std::cout << "short write in Put" << std::endl;
    if (write(_logfile, key.data(), ksize) != ksize)
        std::cout << "short write in Put" << std::endl;
    if (write(_logfile, value.data(), vsize) != vsize)
        std::cout << "short write in Put" << std::endl;

    return _db->Put(options, key, Slice(std::to_string(log_offset)));
}

Status SharedLogDB::PutNoLog(const WriteOptions& options, const Slice& key,
                             const Slice& value) {
    return _db->Put(options, key, value);
}

Status SharedLogDB::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    std::string loc;
    Status s = _db->Get(options, key, &loc);
    assert(s.ok());

    off_t offset = stol(loc);
    size_t ksize;
    size_t vsize;
    
    lseek(_logfile, offset, SEEK_SET);
    assert(read(_logfile, &ksize, sizeof(size_t)) != -1);
    assert(read(_logfile, &vsize, sizeof(size_t)) != -1);

    char *kbuf = (char*)malloc(ksize * sizeof(char));
    char *vbuf = (char*)malloc(vsize * sizeof(char));

    // put in while loops... 
    assert(read(_logfile, kbuf, ksize) != -1);

    if (read(_logfile, vbuf, vsize) != vsize) 
        std::cout << "short read in Get" << std::endl;
    lseek(_logfile, offset, SEEK_END);

    value->assign(vbuf, vsize);

    free(kbuf);
    free(vbuf);
    return s; 
}

SharedLogDB::~SharedLogDB() {
    if (close(_logfile) < 0)
        std::cout << "Error closing logfile" << std::endl;

    delete _db;
}

Status DestroySharedDB(const std::string& name, const std::string& logfile,
                       const Options& options) {
    if (unlink(logfile.c_str()) < 0) 
        std::cout << "Error unlinking file ... proceeding" << std::endl;

    return DestroyDB(name, options);
}
    
int testTwoDBSVLMultiValue() {
    RandomGenerator gen;
    Options dbOptions;
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    dbOptions.create_if_missing = true;
    int value_size = 32;

    std::string logfile = "/tmp/vlog1.txt";
    
    auto db1 = std::make_unique<SharedLogDB>(raftDBPath, logfile, dbOptions);
    auto db2 = std::make_unique<SharedLogDB>(kvDBPath, logfile, dbOptions);

    std::vector<KVPair> kvvec;
    for (int i = 0; i < 10; i++) {
        Slice key = gen.Generate(sizeof(int));
        Slice val = gen.Generate(value_size);
        kvvec.push_back(KVPair(key, val));
    }

    // Put key-value
    Status s;
    off_t o1;
    std::vector<off_t> offsets;
    for (KVPair p : kvvec) {
        s = db1->Put(WriteOptions(), p.GetKey(), p.GetValue(), &o1);
        assert(s.ok());
        offsets.push_back(o1);
    }

    // Put in db2, but don't write values to log
    for (int i = 0; i < offsets.size(); i++) {
        s = db2->PutNoLog(WriteOptions(), kvvec[i].GetKey(), std::to_string(offsets[i]));
        assert(s.ok());
    }

    std::string value;
    // get values. Read backwards just in case there's a bug with only reading
    // sequentially
    for (int i = kvvec.size() - 1; i >= 0; i--) {
        s = db1->Get(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    // and get values from db2, which never inserted values into the log
    for (int i = 0; i < kvvec.size(); i++) {
        s = db2->Get(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    DestroySharedDB(raftDBPath, logfile, dbOptions);
    DestroySharedDB(kvDBPath, logfile, dbOptions);
    return 1;
}

int testTwoDBSVLOneValue() {
    Options dbOptions;
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    dbOptions.create_if_missing = true;

    std::string logfile1 = "/tmp/vlog1.txt";
    
    auto db1 = std::make_unique<SharedLogDB>(raftDBPath, logfile1, dbOptions);
    auto db2 = std::make_unique<SharedLogDB>(kvDBPath, logfile1, dbOptions);

    // Put key-value
    Status s;
    off_t o1;
    s = db1->Put(WriteOptions(), "key1", "value1", &o1);
    assert(s.ok());
    s = db2->PutNoLog(WriteOptions(), "key1", std::to_string(o1));
    assert(s.ok());

    std::string value;
    // get value
    s = db1->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");
    s = db2->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    DestroySharedDB(raftDBPath, logfile1, dbOptions);
    DestroySharedDB(kvDBPath, logfile1, dbOptions);
    return 1;

}

int testTwoDBOneValue() {
    Options dbOptions;
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    dbOptions.create_if_missing = true;

    std::string logfile1 = "/tmp/vlog1.txt";
    std::string logfile2 = "/tmp/vlog2.txt";
    
    auto db1 = std::make_unique<SharedLogDB>(raftDBPath, logfile1, dbOptions);
    auto db2 = std::make_unique<SharedLogDB>(kvDBPath, logfile2, dbOptions);

    // Put key-value
    Status s;
    off_t o1;
    s = db1->Put(WriteOptions(), "key1", "value1", &o1);
    assert(s.ok());
    s = db2->Put(WriteOptions(), "key1", "value1", &o1);
    assert(s.ok());

    std::string value;
    // get value
    s = db1->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");
    s = db2->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    DestroySharedDB(raftDBPath, logfile1, dbOptions);
    DestroySharedDB(kvDBPath, logfile2, dbOptions);
    return 1;
}

int testOneDBOneValue() {
    Options dbOptions;
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    dbOptions.create_if_missing = true;

    std::string logfile = "/tmp/vlog1.txt";
    
    auto raftdb = std::make_unique<SharedLogDB>(raftDBPath, logfile, dbOptions);

    // Put key-value
    Status s;
    off_t o1;
    s = raftdb->Put(WriteOptions(), "key1", "value1", &o1);
    assert(s.ok());

    std::string value;
    // get value
    s = raftdb->Get(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    DestroySharedDB(raftDBPath, logfile, dbOptions);
    return 1;
}

int testOneDBMultiValue() {
    RandomGenerator gen;
    Options dbOptions;
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    dbOptions.create_if_missing = true;
    int value_size = 32;

    std::string logfile = "/tmp/vlog1.txt";
    
    auto raftdb = std::make_unique<SharedLogDB>(raftDBPath, logfile, dbOptions);

    std::vector<KVPair> kvvec;
    for (int i = 0; i < 10; i++) {
        // they generate keys sort of like this in db_bench_tool.cc...
        // char* data = new char[sizeof(int)];
        // std::unique_ptr<const char[]> k(data);
        // Slice key(k.get(), sizeof(int));
        // char* keystart = const_cast<char*>(key.data());
        // memcpy(keystart, static_cast<void*>(&i), sizeof(int));

        Slice key = gen.Generate(sizeof(int));
        Slice val = gen.Generate(value_size);
        kvvec.push_back(KVPair(key, val));
    }

    // Put key-value
    Status s;
    off_t o1;
    for (KVPair p : kvvec) {
        s = raftdb->Put(WriteOptions(), p.GetKey(), p.GetValue(), &o1);
        assert(s.ok());
    }

    std::string value;
    // get values. Read backwards just in case there's a bug with only reading
    // sequentially
    for (int i = kvvec.size() - 1; i >= 0; i--) {
        s = raftdb->Get(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    DestroySharedDB(raftDBPath, logfile, dbOptions);
    return 1;
}

int main() {
    assert(testOneDBOneValue() == 1);
    assert(testOneDBMultiValue() == 1);
    assert(testTwoDBOneValue() == 1);
    assert(testTwoDBSVLOneValue() == 1);
    assert(testTwoDBSVLMultiValue() == 1);

    return 0;
}
