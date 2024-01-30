#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <vector>
#include <thread>
#include <fstream>

using namespace std;

void read(const string& file, std::vector<string>& lines, uint32_t first, uint32_t chunks)
{
    std::string line;

    ifstream f{file};
    uint32_t lineNumber = 1;
    if(!f)
        std::cout << "could not open file" << std::endl;
    
    while (std::getline(f, line) && lineNumber <= first * chunks + chunks)
    {
        if(!line.empty() && lineNumber >= first)
            lines.push_back(line);
    }

}

void mapper(string file, std::vector<std::pair<string, double>>& map, uint32_t first, uint32_t chunks)
{
    std::vector<string> lines;
    std::string line;

    ifstream f{file};
    uint32_t lineNumber = first * chunks;
    if(!f)
        std::cout << "could not open file" << std::endl;
    
    while (std::getline(f, line) && lineNumber <= first * chunks + chunks - 1)
    {
        if(!line.empty() && lineNumber >= first)
        {
            size_t pos = line.find(';');
            map.push_back({line.substr(0, pos), stod(line.substr(pos+1))});
        }
        lineNumber++;

    }
}

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    std::vector<std::pair<string, double>> maps[cpus];
    std::vector<thread> threads;

    if (argc > 1)
        file = argv[1];

   // std::cout << file << " " << cpus << " " << lines.size() << std::endl;

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

    std::cout << "Total entries: " << res - 1 << std::endl;
    return 0;
}