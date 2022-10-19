#ifndef RAFTLOG_H
#define RAFTLOG_H

#include <mutex>
#include <queue>
#include <condition_variable>

using namespace rocksdb;

class RaftEntry
{
public:
    RaftEntry() {}
    RaftEntry(Slice& key, Slice& value)
        : _key(key),
          _value(value) {}

    void set_key(Slice& key) { _key = key; }
    void set_value(Slice& value) { _value = value; }
    Slice get_key() const { return _key; }
    Slice get_value() const { return _value; }

private:
    Slice _key;
    Slice _value;
};

class RaftLog
{
public:
    RaftLog(std::size_t cap) : _cap(cap), _total(0) {}

    std::size_t size() { std::lock_guard<std::mutex> g(lock); return log.size(); }
    std::size_t capacity() { std::lock_guard<std::mutex> g(lock); return _cap; }
    std::size_t get_total() { std::lock_guard<std::mutex> g(lock); return _total; }

    void push(const RaftEntry&);
    std::unique_ptr<RaftEntry> pop();

private:
    std::queue<RaftEntry> log;

    mutable std::mutex lock;
    std::condition_variable full;
    std::condition_variable empty;
    std::size_t _cap;
    std::size_t _total;
};

void RaftLog::push(const RaftEntry& e) {
    std::unique_lock<std::mutex> lk(lock);

    empty.wait(lk, [this]{ return log.size() != _cap; });
    log.push(e);
    full.notify_one();
}

std::unique_ptr<RaftEntry> RaftLog::pop() {
    std::unique_lock<std::mutex> lk(lock);

    full.wait(lk, [this]{ return !log.empty(); });
    auto ret = std::make_unique<RaftEntry>(log.front());
    _total++;
    log.pop();

    empty.notify_one();
    return ret;
    
}

    
#endif // RAFTLOG_H
