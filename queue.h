#ifndef THREADSAFE__QUEUE_H_
#define THREADSAFE__QUEUE_H_

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

template <typename T>
class Queue {
  struct Node {
    T value;
    std::unique_ptr<Node> next;
  };

 public:
  Queue() : head_(std::make_unique<Node>()), tail_(head_.get()) {}

  void Push(T value);
  T WaitAndPop();
  std::optional<T> TryPop();
  [[nodiscard]] bool Empty() const;

 private:
  Node* GetTail();

  std::unique_ptr<Node> head_;
  Node* tail_;
  mutable std::mutex hm_;
  mutable std::mutex tm_;
  std::condition_variable cv_;
};

template <typename T>
void Queue<T>::Push(T value) {
  std::unique_ptr<Node> node = std::make_unique<Node>();
  {
    std::scoped_lock lock(tm_);
    tail_->value = std::move(value);
    tail_->next = std::move(node);
    tail_ = tail_->next.get();
  }
  cv_.notify_one();
}

template <typename T>
T Queue<T>::WaitAndPop() {
  std::unique_lock lock(hm_);
  cv_.wait(lock, [this]() { return head_.get() != GetTail(); });
  auto val = std::move(head_->value);
  head_ = std::move(head_->next);
  return val;
}

template <typename T>
std::optional<T> Queue<T>::TryPop() {
  std::scoped_lock lk(hm_);
  if (head_.get() == GetTail()) {
    return std::nullopt;
  }
  auto val = std::move(head_->value);
  head_ = std::move(head_->next);
  return val;
}

template <typename T>
bool Queue<T>::Empty() const {
  std::scoped_lock lock(hm_, tm_);
  return head_.get() == tail_;
}

template <typename T>
typename Queue<T>::Node* Queue<T>::GetTail() {
  std::scoped_lock lock(tm_);
  return tail_;
}

#endif  // THREADSAFE__QUEUE_H_
