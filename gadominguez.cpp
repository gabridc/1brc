#include <iostream>
#include <string>
#include <numeric>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <vector>
#include <thread>
#include <fstream>
#include <string.h>
#include <queue>
#include <map>
#include <chrono>
#include <ranges>

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

// Build command: g++ -O2 gadominguez.cpp -o gadominguez -std=c++23

void writeOutput(std::map<string, std::vector<double>>& shufflemap, std::map<string, std::tuple<double, double, double>>& output, 
                bool multiThread, const unsigned int cpus, const int totalEntries,
                std::chrono::duration<float,std::milli> mapperDuration,
                std::chrono::duration<float,std::milli> shuffleDuration,
                std::chrono::duration<float,std::milli> totalDuration)
{
    ofstream fout{"out-" + to_string(multiThread) + ".txt", std::ios::out}; 
    for(auto [city, values] : shufflemap)
    {
        string str = city + " : " + to_string(values.size()) + " [ " ;
        bool first = true;
        for(auto v : values)
        {
            if(first)
                str += to_string(v);
            else
            {
                str += ", " + to_string(v);
            }
            first = false;

        }
        str += "]";
        

        fout << str << std::endl;
    }
    fout.close();

    ofstream foutSum{"out-summary-" + to_string(multiThread) + ".txt", std::ios::out};
    
    foutSum << "------------------------------" << std::endl;
    foutSum << "Number of cores: " << cpus << std::endl 
    << "Multithread: "  <<  multiThread <<  std::endl 
    << "Total Entries: "  <<  totalEntries <<  std::endl 
    << "Mapper duration: "  <<  mapperDuration.count() / 1000  << " s"  <<  std::endl 
    << "Shuffle duration: " << shuffleDuration.count() / 1000  << " s" <<  std::endl
    << "Reduce duration: " << shuffleDuration.count() / 1000  << " s" <<  std::endl
    << "Total duration: "  <<  totalDuration.count() / 1000  << " s" << std::endl;
    foutSum << "------------------------------" << std::endl;

    for(auto [city, values] : output)
    {
        foutSum <<  city + " : [ "  + to_string(std::get<0>(values)) + "," + to_string(std::get<1>(values)) + "," + to_string(std::get<2>(values)) + "]" << std::endl;
    }
    foutSum.close();
}

#include <unistd.h>
void writePreformance(bool multiThread, const unsigned int cpus, const unsigned long long totalEntries,
                std::chrono::duration<float,std::milli> mapperDuration,
                std::chrono::duration<float,std::milli> shuffleDuration,
                std::chrono::duration<float,std::milli> totalDuration)
{
     char hostname[256];
    gethostname(hostname, sizeof(hostname));
    ofstream foutSum{std::string(hostname) + "-summary" + to_string(multiThread) + ".txt", std::ios::out};
    
    foutSum << std::string(hostname) << std::endl;
    foutSum << "------------------------------" << std::endl;
    foutSum << "Number of cores: " << cpus << std::endl 
    << "Multithread: "  <<  multiThread <<  std::endl 
    << "Total Entries: "  <<  totalEntries <<  std::endl 
    << "Mapper duration: "  <<  mapperDuration.count() / 1000  << " s"  <<  std::endl 
    << "Shuffle duration: " << shuffleDuration.count() / 1000  << " s" <<  std::endl
    << "Reduce duration: " << shuffleDuration.count() / 1000  << " s" <<  std::endl
    << "Total duration: "  <<  totalDuration.count() / 1000  << " s" << std::endl;
    foutSum << "------------------------------" << std::endl;

    foutSum.close();
}


double s2d(const std::string& str) {
    double result = 0.0;
    int integerPart = 0;
    double decimalPart = 0.0;
    int decimalCount = 0;
    bool isNegative = false;
    bool inDecimal = false;

    for (char c : str) {
        if (c == '-') {
            isNegative = true;
        } else if (c == '.') {
            inDecimal = true;
        } else {
            if (!inDecimal) {
                integerPart = integerPart * 10 + (c - '0');
            } else {
                decimalPart = decimalPart * 10 + (c - '0');
                decimalCount++;
            }
        }
    }

    result = integerPart + decimalPart / pow(10, decimalCount);

    if (isNegative) {
        result *= -1;
    }

    return result;
}

unsigned long long findFirstCharLine(const char* buffer, unsigned long long pos)
{
    char character = '\0';
    unsigned long long p = pos;
    
    if((char) buffer[p] == '\n')
        p--;

    while(buffer[p] != '\n')
        p--;
    return p + 1;
}

unsigned long long findLastCharLine(const char* buffer, unsigned long long pos)
{
    char character = '\0';
    unsigned long long p = pos;
    
    if((char) buffer[p] == '\n')
        p--;

    while(buffer[p] != '\n')
        p--;

    return p;
}

void readLine(const char* buffer, string& line, unsigned long long& initPos, unsigned long long& size)
{
    line.clear();
    do
    {
        if((char) buffer[initPos] != '\n')
            line += buffer[initPos];
        initPos++;
    } while ( (char) buffer[initPos] != '\n' && initPos < size);

}

#define MAX_DISTINCT_GROUPS 10000
#define MAX_GROUPBY_KEY_LENGTH 100
#define HCAP (1 << 14)

struct Group {
  unsigned int count;
  long sum;
  int min;
  int max;
  char key[MAX_GROUPBY_KEY_LENGTH];
};

struct Result {
  unsigned int map[HCAP];
  unsigned int hashes[HCAP];
  unsigned int n;
  struct Group groups[MAX_DISTINCT_GROUPS];
};

struct Chunk {
  size_t start;
  size_t end;
  const char *data;
};

static inline const char *parse_number(int *dest, const char *s) {
  // parse sign
  int mod;
  if (*s == '-') {
    mod = -1;
    s++;
  } else {
    mod = 1;
  }

  if (s[1] == '.') {
    *dest = ((s[0] * 10) + s[2] - ('0' * 11)) * mod;
    return s + 4;
  }

  *dest = (s[0] * 100 + s[1] * 10 + s[3] - ('0' * 111)) * mod;
  return s + 5;
}

static void *process_chunk(void *ptr) {
  struct Chunk *ch = (struct Chunk *)ptr;

  // skip start forward until SOF or after next newline
  if (ch->start > 0) {
    while (ch->data[ch->start - 1] != '\n') {
      ch->start++;
    }
  }

  while (ch->data[ch->end] != 0x0 && ch->data[ch->end - 1] != '\n') {
    ch->end++;
  }

  struct Result *result = static_cast<Result*>(malloc(sizeof(*result)));
  if (!result) {
    perror("malloc error");
    exit(EXIT_FAILURE);
  }
  result->n = 0;

  memset(result->hashes, 0, HCAP * sizeof(int));
  memset(result->map, 0, HCAP * sizeof(int));

  const char *s = &ch->data[ch->start];
  const char *end = &ch->data[ch->end];
  const char *linestart;
  unsigned int h;
  int temperature;
  int len;
  unsigned int c;

  while (s != end) {
    linestart = s;

    // hash everything up to ';'
    // assumption: key is at least 1 char
    len = 1;
    h = (unsigned char)s[0];
    while (s[len] != ';') {
      h = (h * 31) + (unsigned char)s[len++];
    }

    // parse decimal number as int
    s = parse_number(&temperature, s + len + 1);

    // probe map until free spot or match
    while (result->hashes[h & (HCAP - 1)] != 0 &&
           h != result->hashes[h & (HCAP - 1)]) {
      h++;
    }
    auto k =  h & (HCAP - 1);
    c = result->map[h & (HCAP - 1)];

    if (c == 0) {
      memcpy(result->groups[result->n].key, linestart, (size_t)len);
      result->groups[result->n].key[len] = 0x0;
      result->groups[result->n].count = 1;
      result->groups[result->n].sum = temperature;
      result->groups[result->n].min = temperature;
      result->groups[result->n].max = temperature;
      result->map[h & (HCAP - 1)] = result->n++;
      result->hashes[h & (HCAP - 1)] = h;
    } else {
      result->groups[c].count += 1;
      result->groups[c].sum += temperature;
      if (temperature < result->groups[c].min) {
        result->groups[c].min = temperature;
      } else if (temperature > result->groups[c].max) {
        result->groups[c].max = temperature;
      }
    }
  }

  return (void *)result;
}

void mapper2(const char* buffer, std::map<string, std::vector<double>>& map, unsigned long long start, unsigned long long end, int core, unsigned long long size)
{
    string line;
    char delim = '\n';
    uint8_t index = 0;  
    unsigned long long initPos = 0;

    if(start !=0)
        initPos = findFirstCharLine(buffer, start);
    
    unsigned long long lastPos = findLastCharLine(buffer, end);

    while (initPos <= lastPos)
    {   
        readLine(buffer, line, initPos, lastPos);
        if(line.size() > 1)
        {
            size_t pos = line.find(';');
            try
            {
                const string& city = line.substr(0, pos);
                const double& value = s2d(line.substr(pos+1));
                auto it = map.find(city);

                if (it == map.end())
                {
                    // I have decided to implement my own s2d, std::stod() has poor performance in multithreading
                    map.emplace(city, std::vector<double>{value});
                }
                else
                {
                    it->second.push_back(value);        
                }   

            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << " " <<line << '\n';
            }
        }
    }
}

void mapper(const char* buffer, std::unordered_map<string, std::vector<double>>& map, unsigned long long start, unsigned long long end, int core, unsigned long long size)
{
    string line;
    char delim = '\n';
    uint8_t index = 0;  
    unsigned long long initPos = 0;

    if(start !=0)
        initPos = findFirstCharLine(buffer, start);
    
    unsigned long long lastPos = findLastCharLine(buffer, end);

    while (initPos <= lastPos)
    {   
        readLine(buffer, line, initPos, lastPos);
        if(line.size() > 1)
        {
            size_t pos = line.find(';');
            try
            {
                const string& city = line.substr(0, pos);
                const double& value = s2d(line.substr(pos+1));
                auto it = map.find(city);

                if (it == map.end())
                {
                    // I have decided to implement my own s2d, std::stod() has poor performance in multithreading
                    map.emplace(city, std::vector<double>{value});
                }
                else
                {
                    it->second.push_back(value);        
                }   

            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << " " <<line << '\n';
            }
        }
    }
}

void reduceFor(std::vector<string>& keys, std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, unsigned long long start, unsigned long long end)
{
    for(unsigned long long index = start; index <= end; index++)
    {
        auto size = map[keys[index]].size();
        if(size != 0)
        {
            const std::vector<double> values = map[keys[index]];
            double min, max, acc = 0.0;
            for(int i = 0; i < size; i++)
            {
               if(i == 0)
                {
                    min = max = acc = values[i];
                }
                else
                {
                    min = (min > values[i]) ? values[i] : min;
                    max = (max < values[i]) ? values[i] : max;
                    acc += values[i];
                }

            }
            double average = acc / size;
            output.emplace(keys[index], std::tuple(min, average, max));
        }

    }
}

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    std::unordered_map<string, std::vector<double>> maps[cpus];
    std::map<string, std::vector<double>> maps2[cpus];
    std::queue<std::pair<string, double>> queue;
    std::vector<thread> threads;
    bool multiThread = false;

    if (argc > 2)
    {
        file = argv[1];
        multiThread = (string(argv[2]) == "1") ? true : false;
    }

    auto startTotal= std::chrono::system_clock::now();

    int fd = open(file.c_str(), O_RDONLY);
    if (!fd) {
        perror("error opening file");
        exit(EXIT_FAILURE);
    }
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("error getting file size");
        exit(EXIT_FAILURE);
    }
  
    size_t size = (size_t)sb.st_size;
    const char *buffer = static_cast<const char*>(mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0));
    if (buffer == MAP_FAILED) {
        perror("error mmapping file");
        exit(EXIT_FAILURE);
    }

    ////////////////////////////////
    
    //  Mapper
    
    /////////////////////////////////

    auto chunkSize = size / cpus;
    auto start = std::chrono::system_clock::now();
    struct Chunk chunks[1];
    chunks->data = buffer;
    chunks->start = 0;
    chunks->end = size;
    if(!multiThread)
    {
        /*auto start1 = std::chrono::system_clock::now();

        process_chunk(chunks);

        auto end1 = std::chrono::system_clock::now();
        std::chrono::duration<float,std::milli> duration1 = end1 - start1;
        std::cout << "Process chunk Duration: "<< duration1.count() << " ms" << std::endl;*/


        //auto start2 = std::chrono::system_clock::now();

        // TODO: Hablar sobre por que el uso de unordered_map es más rapido.
        // Ref: https://julien.jorge.st/posts/en/effortless-performance-improvements-in-cpp-std-unordered_map/
        mapper(buffer, ref(maps[0]), 0, size, 0, size);
        /*
        auto end2 = std::chrono::system_clock::now();
        std::chrono::duration<float,std::milli> duration2 = end2 - start2;
        std::cout << "Mapper std::unordered_map Duration: "<< duration2.count() << " ms" << std::endl;
        */
    
        /*auto start3 = std::chrono::system_clock::now();

        mapper2(buffer, ref(maps2[0]), 0, size, 0, size);

        auto end3 = std::chrono::system_clock::now();
        std::chrono::duration<float,std::milli> duration3 = end3 - start3;
        std::cout << "Mapper std::map Duration: "<< duration3.count() << " ms" << std::endl;*/
    }

    else
    {
        for(uint8_t core = 0; core < cpus; core++)
        {
            unsigned long start = core * chunkSize;
            unsigned long end = (core == cpus - 1) ? end = size : (core + 1) * chunkSize - 1;
            threads.emplace_back(mapper, buffer, ref(maps[core]), start, end, core, size);
        }
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<float,std::milli> duration = end - start;

        for(auto& t : threads)
            t.join();
        threads.clear();
    }

    auto mapperEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> mapperDuration = mapperEnd - start;
    std::cout << "Mapper Duration: "<< mapperDuration.count() / 1000 << " s" << std::endl;
    ////////////////////////////////
    
    //  Shuffle
    
    /////////////////////////////////
    auto startShuffle = std::chrono::system_clock::now();

    unsigned long long totalEntries = 0;
    std::map<string, std::vector<double>> shufflemap;
    std::vector<string> keys;
    for(const auto& map : maps)
        for(const auto& [city, values] : map)
        {
                auto it = shufflemap.find(city);
                totalEntries += values.size();                  
                
                if (it == shufflemap.end())
                {
                    // I have decided to implement my own s2d, std::stod() has poor performance in multithreading
                    shufflemap.emplace(city, values);
                    keys.push_back(city);
                }
                else
                {
                    shufflemap[city].insert(shufflemap[city].end(), values.begin(), values.end());   
   
                }  
        }

    std::cout << "Total entries: " << totalEntries << std::endl;

    auto endShuffle = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> shuffleDuration = endShuffle - startShuffle;
    std::cout << "Shuffle duration: " << shuffleDuration.count() / 1000 << " s" << std::endl;

    ////////////////////////////////
    
    //  Reduce
    
    /////////////////////////////////
    auto reduceStart = std::chrono::system_clock::now();
    std::map<string, std::tuple<double, double, double>> output;

    if(!multiThread)
        reduceFor(ref(keys), ref(shufflemap), ref(output), 0, keys.size());
    else
    {
        size = keys.size() / cpus;
        for(uint8_t core = 0; core < cpus; core++)
        {
            unsigned long start = core * size;
            unsigned long end = (core == cpus - 1) ? end =  keys.size()  : (core + 1) *  size - 1;
            threads.emplace_back(reduceFor, ref(keys), ref(shufflemap), ref(output), start, end);
        }

        for(auto& t : threads)
            t.join();
    }
    
    auto reduceEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> reduceDuration = reduceEnd - reduceStart;
    std::cout << "Reduce duration: " << reduceDuration.count() / 1000 << " s" << std::endl;
    
    auto end1 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> totalDuration = end1 - startTotal;

    std::cout << "Total duration: " << totalDuration.count() / 1000 << " s" << std::endl;

    writePreformance(multiThread, cpus, totalEntries, mapperDuration, shuffleDuration, totalDuration);
    //writeOutput(shufflemap, output, multiThread, cpus, totalEntries, mapperDuration, shuffleDuration, totalDuration);

    return 0;
}

/*void reduceMinMax(std::vector<string>& keys, std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, unsigned long long_t start, unsigned long long_t end)
{
    for(unsigned long long_t index = start; index <= end; index++)
    {
        auto size = map[keys[index]].size();
        if(size != 0)
        {
            auto min = *std::min_element(map[keys[index]].begin(), map[keys[index]].end());
            auto max = *std::max_element(map[keys[index]].begin(), map[keys[index]].end());
            double average = std::accumulate(map[keys[index]].begin(), map[keys[index]].end(), 0.0) / size;
            output.emplace(keys[index], std::tuple(min, average, max));
        }
    }
}*/