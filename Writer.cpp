/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Writer.cpp
 *
 *  Created on: Sep 13, 2019
 *      Author: Jason Wang
 */

#include <adios2.h>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>
#include <mutex>

int totalThreads = 1;
std::string ipAddress = "127.0.0.1";
int port = 50001;

size_t steps = 10000;
adios2::Dims shape = {1000000};
adios2::Dims start = {0};
adios2::Dims count = {1000000};

bool generateDataForEveryStep = false;

std::mutex printLock;

template <class T>
void PrintData(const std::vector<T> &data, const size_t rankStep, const size_t globalStep, const int threadId, const bool verbose)
{
    printLock.lock();
    std::cout << "Thread " << threadId << ", Thread Step " << rankStep << ",  Global Step " << globalStep << std::endl;
    if(verbose)
    {
        std::cout << "[";
        for (const auto i : data)
        {
            std::cout << i << " ";
        }
        std::cout << "]" << std::endl;
    }
    printLock.unlock();
}

template <class T>
std::vector<T> GenerateData(const size_t step, const int threadId)
{
    size_t datasize = std::accumulate(count.begin(), count.end(), 1, std::multiplies<size_t>());
    std::vector<T> myVec(datasize);
    for (size_t i = 0; i < datasize; ++i)
    {
        myVec[i] = i + threadId * 10000 + step;
    }
    return myVec;
}

void Thread(const int threadId)
{
    adios2::ADIOS adios;
    adios2::IO dataManIO = adios.DeclareIO("whatever");
    dataManIO.SetEngine("DataMan");
    dataManIO.SetParameters({{"IPAddress", ipAddress}, {"Port", std::to_string(port + threadId*2)}, {"Timeout", "5"}});

    adios2::Engine dataManWriter = dataManIO.Open("HelloDataMan", adios2::Mode::Write);

    auto floatArrayVar = dataManIO.DefineVariable<float>("FloatArray", shape, start, count);
    auto stepVar = dataManIO.DefineVariable<uint64_t>("GlobalStep");

    auto floatVector = GenerateData<float>(0, threadId);
    for (uint64_t i = threadId; i < steps; i+=totalThreads)
    {
        dataManWriter.BeginStep();
        if(generateDataForEveryStep)
        {
            floatVector = GenerateData<float>(i, threadId);
        }
        dataManWriter.Put(floatArrayVar, floatVector.data());
        dataManWriter.Put(stepVar, i);
        PrintData(floatVector, dataManWriter.CurrentStep(), i, threadId, false);
        dataManWriter.EndStep();
    }
    dataManWriter.Close();
}

int main(int argc, char *argv[])
{
    if(argc > 1)
    {
        totalThreads = atoi(argv[1]);
    }
    if(argc > 2)
    {
        ipAddress = argv[2];
    }
    if(argc > 3)
    {
        port = atoi(argv[3]);
    }

    steps = steps * totalThreads;

    auto startTime = std::chrono::system_clock::now();

    std::vector<std::thread> allThreads;
    for(int i=0; i<totalThreads; ++i)
    {
        allThreads.emplace_back(std::thread(Thread, i));
    }
    for(auto &t : allThreads)
    {
        if(t.joinable())
        {
            t.join();
        }
    }

    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    auto durationSeconds = std::chrono::duration_cast<std::chrono::seconds>(endTime - startTime);
    size_t totalDataBytes = std::accumulate(shape.begin(), shape.end(), sizeof(float) * steps, std::multiplies<size_t>());
    float dataRate = (double)totalDataBytes / (double)duration.count();

    std::cout << "Total steps: " << steps << std::endl;
    std::cout << "Total data sent: " << totalDataBytes / 1000000.0 << " MBs" << std::endl;
    std::cout << "Time: " << durationSeconds.count() << " seconds" << std::endl;
    std::cout << "Data rate: " << dataRate << " MB/s" << std::endl;

    return 0;

}
