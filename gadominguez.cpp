#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <vector>
#include <thread>
#include <fstream>

using namespace std;

uint32_t findFirstCharLine(std::ifstream& f, uint32_t pos)
{
    char character = '\0';
    uint8_t index = 0U;
    uint32_t p = pos;
    
    f.seekg(p);
    if((char)f.peek() == '\n')
        p--;
    f.seekg(p);
    while(f.peek() != '\n')
    {
        f.seekg(p-index);
        index++;
    }

    return (uint32_t) f.tellg() + 1;
}

uint32_t findLastCharLine(std::ifstream& f, uint32_t pos)
{
    char character = '\0';
    uint8_t index = 0U;
    uint32_t p = pos;
    
    f.seekg(p);
    if((char)f.peek() == '\n')
        p--;
    f.seekg(p);
    while(f.peek() != '\n')
    {
        f.seekg(p-index);
        index++;
    }

    return (uint32_t) f.tellg();
}

void mapper(string file, std::vector<std::pair<string, double>>& map, uint32_t start, uint32_t end, int core)
{
    string line;
    ifstream f{file, std::ios::in};
    if(!f)
        std::cout << "could not open file" << std::endl;

    uint8_t index = 0;  
    uint32_t initPos = 0;

    if(start !=0)
        initPos = findFirstCharLine(f, start);
    
    uint32_t lastPos = findLastCharLine(f, end);

    f.seekg(initPos);
    while(f.tellg() <= lastPos)
    {
        getline(f, line);
        size_t pos = line.find(';');
        map.emplace_back(line.substr(0, pos), stod(line.substr(pos+1)));
    }


    /*f.seekg(initPos);
    getline(f, line);
    std::cout << "Thread ID: " << core << " ";
    std::cout << "Pos Init: "<< initPos <<  " Last Pos: " << lastPos << std::endl;
    std::cout << line << std::endl;
    f.seekg(lastPos - 8);
    getline(f, line);
    std::cout << line << std::endl;*/
}

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    std::vector<std::pair<string, double>> maps[cpus];
    std::vector<thread> threads;

    if (argc > 1)
        file = argv[1];

    ifstream f{file, std::ios::in};
    if(!f)
        std::cout << "could not open file" << std::endl;

    auto size = f.tellg();
    f.seekg( 0, std::ios::end );
    size = f.tellg() - size;

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

    std::cout << file << " " << cpus << " " << size << " bytes" << std::endl;
    auto chunkSize = size / cpus;

    std::cout << file << " " << cpus << " " << size << " bytes "  << chunkSize << " bytes"<< std::endl;

    //mapper(file, ref(maps[0]), 0, size, 0);

   for(uint8_t core = 0; core < cpus; core++)
    {
        int start = core * chunkSize;
        int end = (core == cpus - 1) ? end = size : (core + 1) * chunkSize - 1;
            

        std::cout << "thread " << (int)core << " Start: " << start << " End: " << end << std::endl;
       threads.emplace_back(mapper, file, ref(maps[core]), start, end, core);
    }

    for(auto& t : threads)
        t.join();
/*
    std::ofstream out{"out.txt", std::ios::out};
    int res = 1;
    for(auto map : maps)
        for(auto line : map)
        {
            out << line.first << ";" << line.second << std::endl;
            res++;
        }

    std::cout << "Total entries: " << res - 1 << std::endl;


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