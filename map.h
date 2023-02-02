#ifndef THREADSAFE__MAP_H_
#define THREADSAFE__MAP_H_

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
        requires std::is_same_v<Val, Value>
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

  std::optional<Value> operator[](const Key& key);
  std::optional<Value> Find(const Key& key);
  [[nodiscard]] bool Contains(const Key& key) const;

  template <typename Val>
    requires std::is_same_v<Val, Value>
  void Insert(const Key& key, Val&& value);
  bool Erase(const Key& key);

 private:
  static constexpr float kMaxCoef = 0.7F;
  static constexpr int kDefaultSize = 55001;

  uint64_t size_;
  std::vector<Bucket> data_;
  Hash hash_;
  float coef_ = 0;
  // guards map data (size, data, coef)
  mutable std::shared_mutex m_;
};

template <typename Key, typename Value, typename Hash>
Map<Key, Value, Hash>::Map(uint64_t size) : size_(size), data_(size_) {}

template <typename Key, typename Value, typename Hash>
std::optional<Value> Map<Key, Value, Hash>::operator[](const Key& key) {
  std::shared_lock m_lk(m_);
  uint64_t h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::shared_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return std::nullopt;
  }

  Node* n = bucket.head.get();
  m_lk.unlock();
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
  std::shared_lock m_lk(m_);
  uint64_t h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::shared_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return false;
  }
  Node* n = bucket.head.get();
  m_lk.unlock();
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
  requires std::is_same_v<Val, Value>
void Map<Key, Value, Hash>::Insert(const Key& key, Val&& val) {
  std::unique_ptr<Node> new_node =
      std::make_unique<Node>(key, std::forward<Val>(val));
  std::shared_lock m_lk(m_);
  uint64_t h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::unique_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    bucket.head = std::move(new_node);
    return;
  }
  Node* n = bucket.head.get();
  m_lk.unlock();
  if (n->key == key) {
    Node::Swap(*new_node, *n);
    return;
  }
  Node* next = nullptr;
  while ((next = n->next.get()) != nullptr) {
    std::unique_lock n_lk(next->m);
    lk.unlock();
    if (next->key == key) {
      Node::Swap(*new_node, *next);
      return;
    }
    n = next;
    lk = std::move(n_lk);
  }
  n->next = std::move(new_node);
}

template <typename Key, typename Value, typename Hash>
bool Map<Key, Value, Hash>::Erase(const Key& key) {
  std::shared_lock m_lk(m_);
  auto h = hash_(key) % size_;
  auto& bucket = data_[h];
  std::unique_lock lk(bucket.m);
  if (bucket.head == nullptr) {
    return false;
  }
  Node* node = bucket.head.get();
  if (node->key == key) {
    bucket.head = std::move(node->next);
    return true;
  }
  m_lk.unlock();
  Node* next = nullptr;
  while ((next = node->next.get()) != nullptr) {
    std::unique_lock n_lk(next->m);
    if (next->key == key) {
      node->next = std::move(next->next);
      return true;
    }
    lk.unlock();
    node = next;
    lk = std::move(n_lk);
  }
  return false;
}

#endif  // THREADSAFE__MAP_H_
