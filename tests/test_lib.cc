#include <gtest/gtest.h>

#include <optional>
#include <thread>

#include "../map.h"

template <typename T>
bool IsNull(const std::optional<T>& a) {
  return !a.has_value();
}

TEST(Map, MainTest) {
  Map<std::string, int> map(2);
  EXPECT_EQ(map.Contains("aboba"), false);
  map.Insert("aboba", 10);
  EXPECT_EQ(map["aboba"], 10);
  map.Insert("aboba", 5);
  EXPECT_EQ(map["aboba"], 5);
  EXPECT_EQ(map.Contains("aboba"), true);

  EXPECT_EQ(map.Contains("zeleboba"), false);
  map.Insert("zeleboba", 10);
  EXPECT_EQ(map.Contains("zeleboba"), true);
  map.Insert("zeleboba", 5);
  EXPECT_EQ(map["zeleboba"], 5);

  map.Insert("dassyr", 20);
  EXPECT_EQ(map["dassyr"], 20);
  EXPECT_EQ(map["zeleboba"], 5);
  EXPECT_EQ(map["aboba"], 5);

  EXPECT_EQ(map.Erase("aboba"), true);
  EXPECT_EQ(map.Erase("aboba"), false);
  EXPECT_EQ(map.Contains("aboba"), false);
  EXPECT_EQ(IsNull(map["aboba"]), true);
  EXPECT_EQ(map["dassyr"], 20);
  EXPECT_EQ(map["zeleboba"], 5);
}

TEST(Map, MultithreadingTest) {
  Map<std::string, std::string> map{2};
  map.Insert("aboba", "aboba");
  std::vector<std::string> strs{"aboba", "dassyr", "zeleboba", "@!$RSDF",
                                "qwerty"};
  std::vector<std::string> strs_del{"zeleboba", "@!$RSDF", "qwerty"};
  std::vector<std::thread> threads;
  static constexpr int kThreadsCount = 100;
  threads.reserve(kThreadsCount);
  for (int i = 0; i < kThreadsCount; ++i) {
    threads.emplace_back([&map, strs, strs_del]() {
      std::for_each(std::make_move_iterator(strs.begin()),
                    std::make_move_iterator(strs.end()),
                    [&map](const auto& s) { map.Insert(s, s); });
      std::for_each(std::make_move_iterator(strs_del.begin()),
                    std::make_move_iterator(strs_del.end()),
                    [&map](const auto& s) { map.Erase(s); });
    });
  }
  for (int i = 0; i < kThreadsCount; ++i) {
    threads[i].join();
  }
  EXPECT_EQ(map.Contains("aboba"), true);
  EXPECT_EQ(map.Contains("dassyr"), true);
  EXPECT_EQ(map.Contains("zeleboba"), false);
  EXPECT_EQ(map.Contains("@!$RSDF"), false);
  EXPECT_EQ(map.Contains("qwerty"), false);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
