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
#include <algorithm>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "../util/random.h"
#include "raftlog.h"

#define DB_SIZE  (32L * 1024 * 1024 * 1024)
#define RL_SIZE  (128)

using namespace rocksdb;

std::string rootDir = "/mydata";
std::string raftDBPath = rootDir + "/rocksdb_raftdb";
std::string kvDBPath = rootDir + "/rocksdb_kvdb";
Options globalDBOptions;
Random64 myrand(0);
const int key_size_ = sizeof(uint64_t);

enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
};

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



// from db_bench_tool.cc
class KeyGenerator {
public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {
        if (mode_ == UNIQUE_RANDOM) {
            // NOTE: if memory consumption of this approach becomes a concern,
            // we can either break it into pieces and only random shuffle a section
            // each time. Alternatively, use a bit map implementation
            // (https://reviews.facebook.net/differential/diff/54627/)
            values_.resize(num_);
            for (uint64_t i = 0; i < num_; ++i) {
                values_[i] = i;
            }
            std::shuffle(
                values_.begin(), values_.end(),
                std::default_random_engine(static_cast<unsigned int>(0)));
        }
    }

    uint64_t Next() {
        switch (mode_) {
        case SEQUENTIAL:
            return next_++;
        case RANDOM:
            return rand_->Next() % num_;
        case UNIQUE_RANDOM:
            assert(next_ < num_);
            return values_[next_++];
        }
        assert(false);
        return std::numeric_limits<uint64_t>::max();
    }

private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
};

Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
}
// from db_bench_tool.cc. Simplified for my purposes
// If keys_per_prefix_ is 0, the key is simply a binary representation of
// random number followed by trailing '0's
//   ----------------------------
//   |        key 00000         |
//   ----------------------------
void GenerateKeyFromInt(uint64_t v, Slice* key) {
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    int bytes_to_fill = std::min(key_size_, 8);
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
        memset(pos, '0', key_size_ - (pos - start));
    }
}

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

    std::string logfile = rootDir + "/vlog1.txt";
    
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

    std::string logfile1 = rootDir + "/vlog1.txt";
    
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

    std::string logfile1 = rootDir + "/vlog1.txt";
    std::string logfile2 = rootDir + "/vlog2.txt";
    
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

    std::string logfile = rootDir + "/vlog1.txt";
    
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

    std::string logfile = rootDir + "/vlog1.txt";
    
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

int doBenchmark(size_t value_size, bool shared_log, bool wal) {
    std::string log1 = rootDir + "/vlog1.txt";
    std::string log2;
    if (shared_log)
        log2 = log1;
    else
        log2 = rootDir + "/vlog2.txt";

    Options dbOptions;
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    dbOptions.create_if_missing = true;

    auto raftdb = std::make_unique<SharedLogDB>(raftDBPath, log1, dbOptions);
    auto kvdb = std::make_unique<SharedLogDB>(kvDBPath, log2, dbOptions);
    RandomGenerator gen;
    WriteOptions wopts = WriteOptions();
    wopts.disableWAL = !wal;
    
    auto raftlog = std::make_unique<RaftLog>(RL_SIZE);
    size_t nfill = (size_t)DB_SIZE / value_size;
    std::unique_ptr<KeyGenerator> keygen;
    keygen.reset(new KeyGenerator(&myrand, UNIQUE_RANDOM, nfill));

    size_t p1 = nfill / 40;
    clock_t t0 = clock();
    
    // start producer thread
    std::thread producer([&] {
        off_t offset;
        std::unique_ptr<const char[]> key_guard;
        Slice key = AllocateKey(&key_guard);

        for (size_t i = 0; i < nfill; i++) {
            int64_t rand_num = keygen->Next();
            GenerateKeyFromInt(rand_num, &key);
            Slice key = gen.Generate(16);
            Slice val = gen.Generate(value_size);
            Slice o;
            raftdb->Put(WriteOptions(), key, val, &offset);

            RaftEntry e;
            e.set_key(key);
            if (shared_log) {
                o = Slice(std::to_string(offset));
                e.set_value(o);
            }
            else {
                e.set_value(val);
            }

            raftlog->push(e);
        }
    });

    // start consumer thread
    std::thread consumer([&] {
        off_t offset;
        for (size_t i = 0; i < nfill; i++) {
            auto e = raftlog->pop();
            Slice key = e->get_key();
            Slice value = e->get_value();

            if (shared_log)
                kvdb->PutNoLog(wopts, key, value);
            else
                kvdb->Put(wopts, key, value, &offset);
                    
            if (i >= p1) {
                clock_t dt = clock() - t0;
                std::cout << "value_size\t" << value_size << "\tnum_keys\t" << i+1
                          << "\telapsed_time\t" << dt * 1.0e-6 << std::endl;
                p1 += (nfill / 40);
            }
            
        }
    });

    producer.join();
    consumer.join();
    
    return 1;
}

int main(int argc, char *argv[]) {
    // assert(testOneDBOneValue() == 1);
    // assert(testOneDBMultiValue() == 1);
    // assert(testTwoDBOneValue() == 1);
    // assert(testTwoDBSVLOneValue() == 1);
    // assert(testTwoDBSVLMultiValue() == 1);

    
    if (argc < 3) {
        std::cout << "Usage: " << argv[0] << " -v <value_size> [-s] [-w]" << std::endl;
        exit(0);
    }

    size_t value_size;
    bool shared_log = false;
    bool wal = true;
    int arg;

    while ((arg = getopt(argc, argv, "v:sw")) != -1) {
        switch (arg) {
        case 'v':
            value_size = std::stoull(optarg, NULL, 10);
            break;
        case 's':
            shared_log = true;
            break;
        case 'w':
            wal = true;
            break;
        default:
            std::cout << "Illegal command line option, exiting." << std::endl;
            return 1;
        }
    }

    doBenchmark(value_size, shared_log, wal);
    return 0;
}
