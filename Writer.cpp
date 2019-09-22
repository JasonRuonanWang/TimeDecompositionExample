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
#include <mpi.h>
#include <numeric>
#include <thread>
#include <vector>

size_t steps = 1000;
adios2::Dims shape = {1000000};
adios2::Dims start = {0};
adios2::Dims count = {1000000};

bool generateDataForEveryStep = false;

template <class T>
void PrintData(const std::vector<T> &data, const size_t rankStep, const size_t globalStep, const bool verbose)
{
    std::cout << ",  Rank step = " << rankStep << ",  Global step = " << globalStep << std::endl;
    if(verbose)
    {
        std::cout << "[";
        for (const auto i : data)
        {
            std::cout << i << " ";
        }
        std::cout << "]" << std::endl;
    }
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

int main(int argc, char *argv[])
{
    MPI_Init(0,0);

    int port = 12306;
    int threadId = 0;
    int totalThreads = 1;

    auto startTime = std::chrono::system_clock::now();
    {
        // initialize adios2
        adios2::ADIOS adios;
        adios2::IO dataManIO = adios.DeclareIO("whatever");
        dataManIO.SetEngine("DataMan");
        dataManIO.SetParameters({{"IPAddress", "127.0.0.1"}, {"Port", std::to_string(port)}, {"Timeout", "5"}});

        // open stream
        adios2::Engine dataManWriter = dataManIO.Open("HelloDataMan", adios2::Mode::Write);

        // define variable
        auto floatArrayVar = dataManIO.DefineVariable<float>("FloatArray", shape, start, count);
        auto stepVar = dataManIO.DefineVariable<uint64_t>("GlobalStep");

        // write data
        auto floatVector = GenerateData<float>(0, threadId);
        startTime = std::chrono::system_clock::now();
        for (uint64_t i = threadId; i < steps; i+=totalThreads)
        {
            dataManWriter.BeginStep();
            if(generateDataForEveryStep)
            {
                floatVector = GenerateData<float>(i, threadId);
            }
            dataManWriter.Put(floatArrayVar, floatVector.data());
            dataManWriter.Put(stepVar, i);
            PrintData(floatVector, dataManWriter.CurrentStep(), i, false);
            dataManWriter.EndStep();
        }
        dataManWriter.Close();
    }
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    size_t totalDataBytes = std::accumulate(shape.begin(), shape.end(), sizeof(float) * steps, std::multiplies<size_t>());
    float dataRate = (double)totalDataBytes / (double)duration.count();

    std::cout << "data rate = " << dataRate << " MB/s" << std::endl;

    return 0;

}
