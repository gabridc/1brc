#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <vector>
#include <thread>
#include <fstream>
#include <string.h>
#include <deque>

using namespace std;

uint32_t findFirstCharLine(const char* buffer, uint32_t pos)
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

uint32_t findLastCharLine(const char* buffer, uint32_t pos)
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

void readLine(const char* buffer, string& line, uint32_t& initPos, uint32_t& size)
{
    line.clear();
    do
    {
        if((char) buffer[initPos] != '\n')
            line += buffer[initPos];
        initPos++;
    } while ( (char) buffer[initPos] != '\n' && initPos < size);

}

void mapper(const char* buffer, std::vector<std::pair<string, double>>& map, uint32_t start, uint32_t end, int core, uint32_t size)
{
    string line;
    char delim = '\n';
    uint8_t index = 0;  
    uint32_t initPos = 0;

    if(start !=0)
        initPos = findFirstCharLine(buffer, start);
    
    uint32_t lastPos = findLastCharLine(buffer, end);

    while (initPos <= lastPos)
    {   
        readLine(buffer, line, initPos, lastPos);
        if(line.size() > 1)
        {
            size_t pos = line.find(';');
            try
            {
                //la funcion stod es el problema
                map.emplace_back(line.substr(0, pos), stod(line.substr(pos+1)));
            }
            catch(const std::exception& e)
            {
                std::cerr << e.what() << " " <<line << '\n';
            }
        }
    }

    //std::cout << "Thread ID: " << core << " ";
    //std::cout << "Pos Init: "<< initPos <<  " Last Pos: " << lastPos << std::endl;
}

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    std::vector<std::pair<string, double>> maps[cpus];
    std::vector<thread> threads;
    threads.reserve(10);

    if (argc > 1)
        file = argv[1];

    ifstream f{file, std::ios::in | std::ios::ate};

    if(!f)
        std::cout << "could not open file" << std::endl;

    auto size = f.tellg();

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

    //mapper(buffer, ref(maps[0]), 0, size, 0, size);

    for(uint8_t core = 0; core < cpus; core++)
    {
        uint32_t start = core * chunkSize;
        uint32_t end = (core == cpus - 1) ? end = size : (core + 1) * chunkSize - 1;
            

       std::cout << "thread " << (uint32_t) core << " Start: " << start << " End: " << end << std::endl;
       threads.emplace_back(mapper, buffer, ref(maps[core]), start, end, core, size);
    }

    for(auto& t : threads)
        t.join();

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
        t.join();

    int res = 1;
    for(auto map : maps)
        for(auto line : map)
            std::cout << line.first << " " << line.second << " " << res ++ << std::endl;

    std::cout << "Total entries: " << res - 1 << std::endl;*/
    return 0;
}