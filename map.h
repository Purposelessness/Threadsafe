#ifndef THREADSAFE__MAP_H_
#define THREADSAFE__MAP_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class Map {
  struct Bucket {
    struct Node {
      template <typename Val>
        requires std::is_same_v<std::remove_cvref_t<Val>, Value> or
                     std::is_convertible_v<Val, Value>
      Node(const Key& key, Val&& val) : key(key), val(std::forward<Val>(val)) {}
      static void Swap(Node& n1, Node& n2) { std::swap(n1.val, n2.val); }

      Key key;
      Value val;
      mutable std::shared_mutex m;
      std::unique_ptr<Node> next = nullptr;
    };

    // guards list head
    mutable std::shared_mutex m;
    std::unique_ptr<Node> head = nullptr;
  };
  using Node = typename Bucket::Node;

 public:
  explicit Map(uint64_t size = kDefaultSize);
  ~Map() = default;

  Map(const Map& other) = delete;
  Map& operator=(const Map& other) = delete;

  std::optional<Value> operator[](const Key& key);
  std::optional<Value> Find(const Key& key);
  [[nodiscard]] bool Contains(const Key& key) const;

  template <typename Val>
    requires std::is_same_v<std::remove_cvref_t<Val>, Value> or
             std::is_convertible_v<Val, Value> bool
  Insert(const Key& key, Val&& value, bool replace = false);
  bool Erase(const Key& key);

  [[nodiscard]] uint64_t Size() const;

  // Not threadsafe.
  Map(Map&& other) noexcept;
  Map& operator=(Map&& other) noexcept;
  void Resize();
  void Resize(uint64_t new_size);
  void FastInsert(Key&& key, Value&& value);

 private:
  static constexpr int kDefaultSize = 55001;

  uint64_t size_;
  std::atomic<uint64_t> count_ = 0;
  std::vector<Bucket> data_;
  Hash hash_;
};

template <typename Key, typename Value, typename Hash>
Map<Key, Value, Hash>::Map(uint64_t size) : size_(size), data_(size_) {}

template <typename Key, typename Value, typename Hash>
std::optional<Value> Map<Key, Value, Hash>::operator[](const Key& key) {
  uint64_t h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::shared_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return std::nullopt;
  }

  Node* n = bucket.head.get();
  if (n->key == key) {
    std::optional out{n->val};
    return out;
  }
  Node* next = nullptr;
  while ((next = n->next.get()) != nullptr) {
    std::shared_lock n_lk(next->m);
    lk.unlock();
    if (next->key == key) {
      std::optional out{next->val};
      return out;
    }
    n = next;
    lk = std::move(n_lk);
  }
  return std::nullopt;
}

template <typename Key, typename Value, typename Hash>
std::optional<Value> Map<Key, Value, Hash>::Find(const Key& key) {
  auto out = this->operator[](key);
  return out;
}

template <typename Key, typename Value, typename Hash>
bool Map<Key, Value, Hash>::Contains(const Key& key) const {
  uint64_t h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::shared_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return false;
  }
  Node* n = bucket.head.get();
  if (n->key == key) {
    return true;
  }
  Node* next = nullptr;
  while ((next = n->next.get()) != nullptr) {
    std::shared_lock n_lk(next->m);
    lk.unlock();
    if (next->key == key) {
      return true;
    }
    n = next;
    lk = std::move(n_lk);
  }
  return false;
}

template <typename Key, typename Value, typename Hash>
template <typename Val>
  requires std::is_same_v<std::remove_cvref_t<Val>, Value> or
           std::is_convertible_v<Val, Value> bool
Map<Key, Value, Hash>::Insert(const Key& key, Val&& val, bool replace) {
  std::unique_ptr<Node> new_node =
      std::make_unique<Node>(key, std::forward<Val>(val));
  uint64_t h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::unique_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    bucket.head = std::move(new_node);
    ++count_;
    return true;
  }
  Node* n = bucket.head.get();
  if (n->key == key) {
    if (!replace) {
      return false;
    }
    Node::Swap(*new_node, *n);
    return true;
  }
  Node* next = nullptr;
  while ((next = n->next.get()) != nullptr) {
    std::unique_lock n_lk(next->m);
    lk.unlock();
    if (next->key == key) {
      if (!replace) {
        return false;
      }
      Node::Swap(*new_node, *next);
      return true;
    }
    n = next;
    lk = std::move(n_lk);
  }
  n->next = std::move(new_node);
  ++count_;
  return true;
}

template <typename Key, typename Value, typename Hash>
bool Map<Key, Value, Hash>::Erase(const Key& key) {
  auto h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::unique_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return false;
  }
  Node* node = bucket.head.get();
  if (node->key == key) {
    bucket.head = std::move(node->next);
    --count_;
    return true;
  }
  Node* next = nullptr;
  while ((next = node->next.get()) != nullptr) {
    std::unique_lock n_lk(next->m);
    if (next->key == key) {
      node->next = std::move(next->next);
      --count_;
      return true;
    }
    lk.unlock();
    node = next;
    lk = std::move(n_lk);
  }
  return false;
}

template <typename Key, typename Value, typename Hash>
uint64_t Map<Key, Value, Hash>::Size() const {
  return count_;
}

template <typename Key, typename Value, typename Hash>
Map<Key, Value, Hash>::Map(Map&& other) noexcept
    : size_(other.size_), count_(other.count_), data_(std::move(other.data_)) {
  other.size_ = 0;
}

template <typename Key, typename Value, typename Hash>
Map<Key, Value, Hash>& Map<Key, Value, Hash>::operator=(Map&& other) noexcept {
  if (this == &other) {
    return *this;
  }
  size_ = other.size_;
  count_ = other.count_;
  data_ = std::move(other.data_);
  other.size_ = 0;
  return *this;
}

template <typename Key, typename Value, typename Hash>
void Map<Key, Value, Hash>::Resize() {
  Resize(size_ * 2);
}

template <typename Key, typename Value, typename Hash>
void Map<Key, Value, Hash>::Resize(uint64_t new_size) {
  if (new_size == 0) {
    return;
  }
  Map<Key, Value, Hash> new_map(new_size);
  std::for_each(data_.begin(), data_.end(), [&new_map](auto&& bucket) {
    for (Node* node = bucket.head.get(); node != nullptr;
         node = node->next.get()) {
      new_map.FastInsert(std::move(node->key), std::move(node->val));
    }
  });
  *this = std::move(new_map);
}

template <typename Key, typename Value, typename Hash>
void Map<Key, Value, Hash>::FastInsert(Key&& key, Value&& val) {
  auto h = hash_(key) % size_;
  if (data_[h].head == nullptr) {
    data_[h].head = std::make_unique<Node>(std::move(key), std::move(val));
    ++count_;
    return;
  }
  Node* prev = data_[h].head.get();
  if (prev->key == key) {
    prev->val = std::move(val);
    return;
  }
  for (Node* next = prev->next; next != nullptr;
       prev = next, next = next->next) {
    if (next->key == key) {
      next->val = std::move(val);
      return;
    }
  }
  prev->next = std::make_unique<Node>(std::move(key), std::move(val));
  ++count_;
}

#endif  // THREADSAFE__MAP_H_
