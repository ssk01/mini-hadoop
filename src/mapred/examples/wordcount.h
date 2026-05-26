#include <string>
#include <sstream>
#include "mapred/api/mapper.h"

using namespace mini_hadoop;

namespace mini_hadoop {
namespace examples {

class WordCountMapper
    : public mapred::Mapper<LongWritable, UTF8, UTF8, LongWritable> {
 public:
  void Map(const LongWritable&, const UTF8& value,
           mapred::OutputCollector<UTF8, LongWritable>& output) override {
    std::istringstream iss(value.Get());
    std::string word;
    while (iss >> word) {
      output.Collect(UTF8(word), LongWritable(1));
    }
  }
};

class WordCountReducer
    : public mapred::Reducer<UTF8, LongWritable, UTF8, LongWritable> {
 public:
  void Reduce(const UTF8& key, const std::vector<LongWritable>& values,
              mapred::OutputCollector<UTF8, LongWritable>& output) override {
    int64_t sum = 0;
    for (const auto& v : values) sum += v.Get();
    output.Collect(key, LongWritable(sum));
  }
};

}  // namespace examples
}  // namespace mini_hadoop
