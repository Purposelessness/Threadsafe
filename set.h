#ifndef THREADSAFE__SET_H_
#define THREADSAFE__SET_H_

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>

template <typename Value, typename Hash = std::hash<Value>>
class Set {
  struct Bucket {
    struct Node {
      template <typename Val>
        requires std::is_same_v<std::remove_cvref_t<Val>, Value> or
                 std::is_convertible_v<Val, Value>
      explicit Node(Val&& value) : value(std::forward<Val>(value)) {}

      Value value;
      mutable std::shared_mutex m;
      std::unique_ptr<Node> next = nullptr;
    };

    // guards list head
    mutable std::shared_mutex m;
    std::unique_ptr<Node> head = nullptr;
  };
  using Node = typename Bucket::Node;

 public:
  explicit Set(uint64_t size = kDefaultSize);
  ~Set() = default;

  Set(const Set& other) = delete;
  Set& operator=(const Set& other) = delete;

  [[nodiscard]] bool Contains(const Value& value) const;

  template <typename Val>
    requires std::is_same_v<std::remove_cvref_t<Val>, Value> or
             std::is_convertible_v<Val, Value>
  void Insert(Val&& value);
  bool Erase(const Value& value);

  [[nodiscard]] uint64_t Size() const;

  // Not threadsafe.
  Set(Set&& other) noexcept;
  Set& operator=(Set&& other) noexcept;
  void Resize();
  void Resize(uint64_t new_size);
  void FastInsert(Value&& value);

 private:
  static constexpr int kDefaultSize = 55001;

  uint64_t size_;
  std::atomic<uint64_t> count_ = 0;
  std::vector<Bucket> data_;
  Hash hash_;
};

template <typename Value, typename Hash>
Set<Value, Hash>::Set(uint64_t size) : size_(size), data_(size_) {}

template <typename Value, typename Hash>
bool Set<Value, Hash>::Contains(const Value& value) const {
  uint64_t h = hash_(value) % size_;
  auto& bucket = data_[h];
  std::shared_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return false;
  }
  Node* n = bucket.head.get();
  if (n->value == value) {
    return true;
  }
  Node* next = nullptr;
  while ((next = n->next.get()) != nullptr) {
    std::shared_lock n_lk(next->m);
    lk.unlock();
    if (next->value == value) {
      return true;
    }
    n = next;
    lk = std::move(n_lk);
  }
  return false;
}

template <typename Value, typename Hash>
template <typename Val>
  requires std::is_same_v<std::remove_cvref_t<Val>, Value> or
           std::is_convertible_v<Val, Value>
void Set<Value, Hash>::Insert(Val&& value) {
  std::unique_ptr<Node> new_node =
      std::make_unique<Node>(std::forward<Val>(value));
  uint64_t h = hash_(value) % size_;
  auto& bucket = data_[h];
  std::unique_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    bucket.head = std::move(new_node);
    ++count_;
    return;
  }
  Node* n = bucket.head.get();
  if (n->value == value) {
    return;
  }
  Node* next = nullptr;
  while ((next = n->next.get()) != nullptr) {
    std::unique_lock n_lk(next->m);
    lk.unlock();
    if (next->value == value) {
      return;
    }
    n = next;
    lk = std::move(n_lk);
  }
  n->next = std::move(new_node);
  ++count_;
}

template <typename Value, typename Hash>
bool Set<Value, Hash>::Erase(const Value& value) {
  auto h = hash_(value) % size_;
  auto& bucket = data_[h];
  std::unique_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return false;
  }
  Node* node = bucket.head.get();
  if (node->value == value) {
    bucket.head = std::move(node->next);
    --count_;
    return true;
  }
  Node* next = nullptr;
  while ((next = node->next.get()) != nullptr) {
    std::unique_lock n_lk(next->m);
    if (next->value == value) {
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

template <typename Value, typename Hash>
uint64_t Set<Value, Hash>::Size() const {
  return count_;
}

template <typename Value, typename Hash>
Set<Value, Hash>::Set(Set&& other) noexcept
    : size_(other.size_), count_(other.count_), data_(std::move(other.data_)) {
  other.size_ = 0;
}

template <typename Value, typename Hash>
Set<Value, Hash>& Set<Value, Hash>::operator=(Set&& other) noexcept {
  if (*this == other) {
    return *this;
  }
  size_ = other.size_;
  count_ = other.count_;
  data_ = std::move(other.data_);
  other.size_ = 0;
  return *this;
}

template <typename Value, typename Hash>
void Set<Value, Hash>::Resize() {
  Resize(size_ * 2);
}

template <typename Value, typename Hash>
void Set<Value, Hash>::Resize(uint64_t new_size) {
  if (new_size == 0) {
    return;
  }
  Set<Value, Hash> new_map(new_size);
  std::for_each(data_.begin(), data_.end(), [&new_map](auto&& bucket) {
    for (Node* node = bucket.head.get(); node != nullptr;
         node = node->next.get()) {
      new_map.FastInsert(std::move(node->value), std::move(node->value));
    }
  });
  *this = std::move(new_map);
}

template <typename Value, typename Hash>
void Set<Value, Hash>::FastInsert(Value&& value) {
  auto h = hash_(value) % size_;
  if (data_[h].head == nullptr) {
    data_[h].head = std::make_unique<Node>(std::move(value));
    ++count_;
    return;
  }
  Node* prev = data_[h].head.get();
  if (prev->value == value) {
    return;
  }
  for (Node* next = prev->next; next != nullptr;
       prev = next, next = next->next) {
    if (next->value == value) {
      return;
    }
  }
  prev->next = std::make_unique<Node>(std::move(value));
  ++count_;
}

#endif  // THREADSAFE__SET_H_
