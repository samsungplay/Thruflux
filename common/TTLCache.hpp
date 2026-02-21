#pragma once
#include <list>
#include <map>
#include <unordered_map>
#include <optional>

template<typename K, typename V>
class TTLCache {
    struct Entry {
        K key;
        V value;
        std::chrono::steady_clock::time_point expiry;
    };

    std::unordered_map<K, typename std::list<Entry>::iterator> map_;

    std::list<Entry> timeline_;

    int ttlSeconds_;
    std::size_t maxSize_;

    using ExpiryCallback = std::function<void(V value)>;
    ExpiryCallback expiryCallback_;

public:
    TTLCache(const int ttlSeconds, const std::size_t maxSize, ExpiryCallback expiry_callback) : ttlSeconds_(ttlSeconds),
        maxSize_(maxSize), expiryCallback_(std::move(expiry_callback)) {
    }

    void put(const K &key, V value) {
        if (map_.size() >= maxSize_) {
            throw std::runtime_error("TTL Cache error : cannot put more entries");
        }
        if (map_.contains(key)) {
            throw std::runtime_error("TTL Cache error: This key already exists");
        }
        auto expiry = std::chrono::steady_clock::now() + std::chrono::seconds(ttlSeconds_);
        timeline_.push_back({key, std::move(value), expiry});
        map_[key] = std::prev(timeline_.end());
    }

    auto begin() const {
        return map_.begin();
    }
    auto end() const {
        return map_.end();
    }


    std::optional<V> get(const K &key) {
        auto it = map_.find(key);
        if (it == map_.end()) return std::nullopt;

        if (it->second->expiry <= std::chrono::steady_clock::now()) {
            auto nodeIt = it->second;
            V value = nodeIt->value;
            timeline_.erase(nodeIt);
            map_.erase(it);
            expiryCallback_(value);
            return std::nullopt;
        }

        return it->second->value;
    }

    V erase(const K &key) {
        auto it = map_.find(key);
        if (it == map_.end()) {
            return nullptr;
        }
        auto timelineIt = it->second;
        V result = std::move(timelineIt->value);
        timeline_.erase(timelineIt);
        map_.erase(key);
        return result;
    }

    void cleanExpired() {
        auto now = std::chrono::steady_clock::now();

        while (!timeline_.empty() && now > timeline_.front().expiry) {
            auto node = timeline_.begin();
            K key = node->key;
            V value = node->value;
            timeline_.erase(node);
            map_.erase(key);
            expiryCallback_(value);
        }
    }
};
