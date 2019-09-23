/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * Reader.cpp
 *
 *  Created on: Sep 13, 2019
 *      Author: Jason Wang
 */

#include <adios2.h>
#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

int totalThreads = 1;
std::string ipAddress = "127.0.0.1";
int port = 50001;
std::vector<size_t> allSteps;
adios2::Dims globalShape;

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

void Thread(const int threadId)
{
    adios2::ADIOS adios;
    adios2::IO dataManIO = adios.DeclareIO("whatever");
    dataManIO.SetEngine("DataMan");
    dataManIO.SetParameters({{"IPAddress", ipAddress}, {"Port", std::to_string(port + threadId*2)}, {"Timeout", "5"}});

    adios2::Engine dataManReader = dataManIO.Open("HelloDataMan", adios2::Mode::Read);

    std::vector<float> floatVector;
    adios2::Dims shape;
    while (true)
    {
        auto status = dataManReader.BeginStep();
        if (status == adios2::StepStatus::OK)
        {
            ++ allSteps[threadId];

            auto floatArrayVar = dataManIO.InquireVariable<float>("FloatArray");
            shape = floatArrayVar.Shape();
            size_t datasize = std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<size_t>());
            floatVector.resize(datasize);
            dataManReader.Get<float>(floatArrayVar, floatVector.data());

            uint64_t globalStep;
            auto stepVar = dataManIO.InquireVariable<uint64_t>("GlobalStep");
            dataManReader.Get<uint64_t>(stepVar, globalStep);

            dataManReader.EndStep();
            PrintData(floatVector, dataManReader.CurrentStep(), globalStep, threadId, false);
        }
        else if (status == adios2::StepStatus::EndOfStream)
        {
            std::cout << "End of stream" << std::endl;
            break;
        }
    }
    if(threadId == 0)
    {
        globalShape = shape;
    }
    dataManReader.Close();
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

    allSteps.resize(totalThreads);

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
    size_t steps = std::accumulate(allSteps.begin(), allSteps.end(), 1, std::plus<size_t>());
    size_t totalDataBytes = std::accumulate(globalShape.begin(), globalShape.end(), sizeof(float) * steps, std::multiplies<size_t>());
    float dataRate = (double)totalDataBytes / (double)duration.count();

    std::cout << "Total steps: " << steps << std::endl;
    std::cout << "Total data sent: " << totalDataBytes / 1000000.0 << " MBs" << std::endl;
    std::cout << "Time: " << durationSeconds.count() << " seconds" << std::endl;
    std::cout << "Data rate: " << dataRate << " MB/s" << std::endl;

    return 0;
}
