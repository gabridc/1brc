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

using namespace std;

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

    result = integerPart + decimalPart / (10 ^ decimalCount);

    if (isNegative) {
        result *= -1;
    }

    return result;
}

void shuffle(std::vector<std::pair<string, double>> *maps, std::map<string, std::vector<double>>& map, unsigned int cpus)
{
    for(auto index = 0; index < cpus; index++)
        for(auto line : maps[index])
        {
            if (map.find(line.first) != map.end())
                map.at(line.first).push_back(line.second);
            else
                map.emplace(line.first, line.second); 
        }
}

uint32_t findFirstCharLine(const char* buffer, unsigned long pos)
{
    char character = '\0';
    uint8_t index = 0U;
    uint32_t p = pos;
    
    if((char) buffer[p] == '\n')
        p--;

    while(buffer[p] != '\n')
        p--;
    return p + 1;
}

uint32_t findLastCharLine(const char* buffer, unsigned long pos)
{
    char character = '\0';
    uint8_t index = 0U;
    uint32_t p = pos;
    
    if((char) buffer[p] == '\n')
        p--;

    while(buffer[p] != '\n')
        p--;

    return p;
}

void readLine(const char* buffer, string& line, unsigned long& initPos, unsigned long& size)
{
    line.clear();
    do
    {
        if((char) buffer[initPos] != '\n')
            line += buffer[initPos];
        initPos++;
    } while ( (char) buffer[initPos] != '\n' && initPos < size);

}

void mapper(const char* buffer, std::vector<std::pair<string, double>>& map, unsigned long start, unsigned long end, int core, unsigned long size)
{
    string line;
    char delim = '\n';
    uint8_t index = 0;  
    unsigned long initPos = 0;

    if(start !=0)
        initPos = findFirstCharLine(buffer, start);
    
    unsigned long lastPos = findLastCharLine(buffer, end);

    auto start2 = std::chrono::system_clock::now();
    while (initPos <= lastPos)
    {   
        readLine(buffer, line, initPos, lastPos);
        if(line.size() > 1)
        {
            size_t pos = line.find(';');
            try
            {
                // I have decided to implement my own s2d, std::stod() has poor performance in multithreading
                map.emplace_back(line.substr(0, pos), s2d(line.substr(pos+1)));
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << " " <<line << '\n';
            }
        }
    }
    auto end2 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> duration = end2 - start2;
    //std::cout << "Mapper duration: " << duration.count() / 1000 << std::endl;

    //std::cout << "Thread ID: " << core << " ";
    //std::cout << "Pos Init: "<< initPos <<  " Last Pos: " << lastPos << std::endl;
}

void reduceMinMax(std::vector<string>& keys, std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, uint32_t start, uint32_t end)
{
    for(uint32_t index = start; index <= end; index++)
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
}

void reduceFor(std::vector<string>& keys, std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, uint32_t start, uint32_t end)
{
    for(uint32_t index = start; index < end; index++)
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


void reduceRanges(std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, uint32_t start, uint32_t end)
{
    auto result1 = map
        | std::views::values
        | std::views::transform([](auto v)
            {
                auto min = std::ranges::min(v);
                auto max = std::ranges::max(v);
                auto avg = std::ranges::fold_left(v, 0, std::plus<double>()) / v.size();
                return std::make_tuple(min, avg, max);
            });

    auto result2 = map
        | std::views::keys;
    
    for(auto [city, values] : std::views::zip(result2, result1))
    {
        output.emplace(city, values);
    }

}

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    std::vector<std::pair<string, double>> maps[cpus];
    std::queue<std::pair<string, double>> queue;
    std::vector<thread> threads;
    threads.reserve(10);
    bool multiThread = false;

    if (argc > 2)
    {
        file = argv[1];
        multiThread = (string(argv[2]) == "1") ? true : false;
    }

    auto startTotal = std::chrono::system_clock::now();
    ifstream f{file, std::ios::in | std::ios::ate};

    if(!f)
        std::cout << "could not open file" << std::endl;

    unsigned long size = f.tellg();

    char* buffer = new char[size];
    if (!buffer) {
        std::cerr << "Memory allocation failed\n";
        return 1;
    }

    f.seekg(0);
    // Read the content of the file into memory
    if (!f.read(buffer, size)) {
        std::cerr << "Failed to read file\n";
        delete[] buffer;
        return 1;
    }

   //std::cout << file << " " << cpus << " " << size << " bytes" << std::endl;
    auto chunkSize = size / cpus;

    //std::cout << file << " " << cpus << " " << size << " bytes "  << chunkSize << " bytes"<< std::endl;

    auto start = std::chrono::system_clock::now();
    if(!multiThread)
        mapper(buffer, ref(maps[0]), 0, size, 0, size);
    else
    {
        for(uint8_t core = 0; core < cpus; core++)
        {
            unsigned long start = core * chunkSize;
            unsigned long end = (core == cpus - 1) ? end = size : (core + 1) * chunkSize - 1;
                

        //std::cout << "thread " << (uint32_t) core << " Start: " << start << " End: " << end << std::endl;
        threads.emplace_back(mapper, buffer, ref(maps[core]), start, end, core, size);
        }
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<float,std::milli> duration = end - start;
        //std::cout << duration.count() / 1000 << " s" << std::endl;

        for(auto& t : threads)
            t.join();
        threads.clear();
    }

    auto mapperEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> mapperDuration = mapperEnd - start;
    std::cout << "Mapper Duration: "<< mapperDuration.count() / 1000 << " s" << std::endl;

    /*std::ofstream out{"out.txt", std::ios::out};
    int res = 1;
    for(auto map : maps)
        for(auto line : map)
        {
            out << line.first << ";" << line.second << std::endl;
            res++;
        }

    std::cout << "Total entries: " << res - 1 << std::endl;*/

/*
    auto chunks = 100000;
    for(auto jobIndex = 0; jobIndex < cpus; jobIndex++)
    {
        //std::cout << jobIndex * chunks << " " << chunks << std::endl;
        threads.push_back(thread(mapper, file, ref(maps[jobIndex]), jobIndex, chunks));
    }
        
    for(auto& t : threads)
        t.join();*/

    int totalEntries = 0;
    for(auto map : maps)
        for(auto line : map)
            totalEntries++;
            //std::cout << line.first << " " << line.second << " " << res ++ << std::endl;

    //std::cout << "Total entries: " << totalEntries << std::endl;


    // suffle
    auto startShuffle = std::chrono::system_clock::now();

    std::map<string, std::vector<double>> shufflemap;
    std::vector<string> keys;
    for(auto map : maps)
        for(auto line : map)
        {
            auto it = shufflemap.find(line.first); 

            if (it == shufflemap.end())
            {
                shufflemap.emplace(line.first, std::vector<double>{line.second});
                keys.emplace_back(line.first);
            }
            else
            {
                it->second.push_back(line.second);        
            }   
        }
    
    auto endShuffle = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> shuffleDuration = endShuffle - startShuffle;
    std::cout << "Shuffle duration: " << shuffleDuration.count() / 1000 << " s" << std::endl;

    ////////////////////////////////
    
    //  Reduce
    
    /////////////////////////////////
    auto reduceStart = std::chrono::system_clock::now();
    std::map<string, std::tuple<double, double, double>> output;

    if(!multiThread)
        reduceMinMax(ref(keys), ref(shufflemap), ref(output), 0, keys.size());
    else
    {
        size = keys.size() / cpus;
        for(uint8_t core = 0; core < cpus; core++)
        {
            unsigned long start = core * size;
            unsigned long end = (core == cpus - 1) ? end =  keys.size()  : (core + 1) *  size - 1;
            threads.emplace_back(reduceMinMax, ref(keys), ref(shufflemap), ref(output), start, end);
            //threads.emplace_back(reduceFor, ref(keys), ref(shufflemap), ref(output), start, end);
        }

        for(auto& t : threads)
            t.join();
    }
    
    auto reduceEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> reduceDuration = reduceEnd - reduceStart;
    std::cout << "ReduceMinMax duration: " << reduceDuration.count() / 1000 << " s" << std::endl;
        
    ////////////////////////////////////////////////////
    reduceStart = std::chrono::system_clock::now();
    std::map<string, std::tuple<double, double, double>> output4;

    if(!multiThread)
        reduceFor(ref(keys), ref(shufflemap), ref(output4), 0, keys.size());  
    
    reduceEnd = std::chrono::system_clock::now();
    reduceDuration = reduceEnd - reduceStart;
    std::cout << "ReduceFor duration: " << reduceDuration.count() / 1000 << " s" << std::endl;    
        
        
    ////////////////////////////////////////////////////    
    reduceStart = std::chrono::system_clock::now();
    std::map<string, std::tuple<double, double, double>> output3;

    if(!multiThread)
        reduceRanges(ref(shufflemap), ref(output3), 0, keys.size());
    
    reduceEnd = std::chrono::system_clock::now();
    reduceDuration = reduceEnd - reduceStart;
    std::cout << "ReduceRanges duration: " << reduceDuration.count() / 1000 << " s" << std::endl;

    ///////////////////////////
    
    auto end1 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> totalDuration = end1 - startTotal;

    std::cout << "Total duration: " << totalDuration.count() / 1000 << " s" << std::endl;

    writeOutput(shufflemap, output, multiThread, cpus, totalEntries, mapperDuration, shuffleDuration, totalDuration);

    return 0;
}