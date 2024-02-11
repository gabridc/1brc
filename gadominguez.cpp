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

using namespace std;

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
                //la funcion stod es el problema
                //string str = line.substr(0, pos) + " + " + line.substr(pos+1);
                map.emplace_back(line.substr(0, pos), stod(line.substr(pos+1)));
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << " " <<line << '\n';
            }
        }
    }
    auto end2 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> duration = end2 - start2;
    std::cout << "Mapper duration: " << duration.count() / 1000 << std::endl;

    //std::cout << "Thread ID: " << core << " ";
    //std::cout << "Pos Init: "<< initPos <<  " Last Pos: " << lastPos << std::endl;
}

void reduce2(std::vector<string>& keys, std::map<string, std::vector<double>>& map,  std::map<string, std::tuple<double, double, double>>& output, uint32_t start, uint32_t end)
{
    for(uint32_t index = start; index < end; index++)
    {
        auto size = map[keys[index]].size();
        std::sort(map[keys[index]].begin(), map[keys[index]].end());

        double suma = std::accumulate(map[keys[index]].begin(), map[keys[index]].end(), 0.0);
        double average = suma / size;

        output.emplace(keys[index], std::tuple(map[keys[index]][0], average, map[keys[index]][size - 1]));
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
        std::cout << duration.count() / 1000 << " s" << std::endl;

        for(auto& t : threads)
            t.join();
        threads.clear();
    }

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

    int res = 1;
    for(auto map : maps)
        for(auto line : map)
            res++;
            //std::cout << line.first << " " << line.second << " " << res ++ << std::endl;

    std::cout << "Total entries: " << res - 1 << std::endl;


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
    std::chrono::duration<float,std::milli> durShuffle = endShuffle - startShuffle;
    std::cout << "Shuffle duration: " << durShuffle.count() / 1000 << " s" << std::endl;

    //Reduce
    auto reduceStart = std::chrono::system_clock::now();
    std::map<string, std::tuple<double, double, double>> output;

    size = keys.size() / cpus;
    for(uint8_t core = 0; core < cpus; core++)
    {
        unsigned long start = core * size;
        unsigned long end = (core == cpus - 1) ? end =  keys.size()  : (core + 1) *  size - 1;
        threads.emplace_back(reduce2, ref(keys), ref(shufflemap), ref(output), start, end);
    }

    for(auto& t : threads)
        t.join();
    
    auto reduceEnd = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> reducedur = reduceEnd - reduceStart;
    std::cout << "Reduce duration: " << reducedur.count() / 1000 << " s" << std::endl;

    auto end1 = std::chrono::system_clock::now();
    std::chrono::duration<float,std::milli> duration1 = end1 - startTotal;

    std::cout << "Total duration: " << duration1.count() / 1000 << " s" << std::endl;

    ofstream fout{"out.txt", std::ios::out}; 
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

    ofstream foutSum{"out-summary.txt", std::ios::out}; 
    for(auto [city, values] : output)
    {
        foutSum <<  city + " : [ "  + to_string(std::get<0>(values)) + "," + to_string(std::get<1>(values)) + "," + to_string(std::get<2>(values)) + "]" << std::endl;
    }
    foutSum.close();

    return 0;
}