#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <vector>
#include <thread>
#include <fstream>

using namespace std;

void read(const string& file, std::vector<string>& lines)
{
    std::string line;

    ifstream f{file};
    if(!f)
        std::cout << "could not open file" << std::endl;
    while (std::getline(f, line))
    {
        if(!line.empty())
            lines.push_back(line);
    }

}

void mapper(const std::vector<string>& lines, std::vector<std::pair<string, double>>& map, uint32_t first, uint32_t last)
{
    for(uint32_t i = first; i <= last; i++)
    {
       size_t pos = lines[i].find(';');
        map.push_back({lines[i].substr(0, pos), stod(lines[i].substr(pos+1))});
    }
}

int main(int argc, char **argv) {
    const unsigned int cpus = std::thread::hardware_concurrency();
    string file = "measurements.txt";
    vector<string> lines;
    std::vector<std::pair<string, double>> maps[cpus];

    if (argc > 1)
        file = argv[1];

    read(file, lines);
   // std::cout << file << " " << cpus << " " << lines.size() << std::endl;

    auto chunks = lines.size() / cpus;
    for(auto jobIndex = 0; jobIndex < cpus; jobIndex++)
    {
        if(jobIndex == 0)
        {
            //std::cout << jobIndex * chunks << " " << chunks << std::endl;
            mapper(lines, maps[jobIndex], 0, chunks);
        }
        else if(jobIndex != cpus - 1)
        {
            //std::cout << jobIndex * chunks + jobIndex << " " <<  (jobIndex * chunks + jobIndex) + chunks << std::endl;
            mapper(lines, maps[jobIndex], jobIndex * chunks + jobIndex, (jobIndex * chunks + jobIndex) + chunks );
        }
        else
        {
            //std::cout << jobIndex * chunks + jobIndex << " " <<  lines.size() - 1 << std::endl;
            mapper(lines, maps[jobIndex], jobIndex * chunks + jobIndex , lines.size() - 1);
        }

    }
        
    /*int res = 0;
    for(int i = 0; i < cpus; i++)
    {

        for(auto s : maps[i])
        {
            std::cout << i << " " << s.first << " " << s.second  << " " << res++ <<  std::endl;
        }

    }*/


    return 0;
}