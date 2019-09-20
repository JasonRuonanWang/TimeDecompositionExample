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

int mpiRank, mpiSize;

template <class T>
void PrintData(const std::vector<T> &data, const size_t rankStep, const size_t globalStep, const bool verbose)
{
    std::cout << "Rank = " << mpiRank << ",  Rank step = " << rankStep << ",  Global step = " << globalStep << std::endl;
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
std::vector<T> GenerateData(const size_t step)
{
    size_t datasize = std::accumulate(count.begin(), count.end(), 1, std::multiplies<size_t>());
    std::vector<T> myVec(datasize);
    for (size_t i = 0; i < datasize; ++i)
    {
        myVec[i] = i + mpiRank * 10000 + step;
    }
    return myVec;
}

int main(int argc, char *argv[])
{
    // initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpiRank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpiSize);

    int port = 12306 + mpiRank*2;

    auto startTime = std::chrono::system_clock::now();
    {
        // initialize adios2
        adios2::ADIOS adios(MPI_COMM_SELF, adios2::DebugON);
        adios2::IO dataManIO = adios.DeclareIO("whatever");
        dataManIO.SetEngine("DataMan");
        dataManIO.SetParameters({{"IPAddress", "127.0.0.1"}, {"Port", std::to_string(port)}, {"Timeout", "5"}});

        // open stream
        adios2::Engine dataManWriter = dataManIO.Open("HelloDataMan", adios2::Mode::Write);

        // define variable
        auto floatArrayVar = dataManIO.DefineVariable<float>("FloatArray", shape, start, count);
        auto stepVar = dataManIO.DefineVariable<uint64_t>("GlobalStep");

        // write data
        auto floatVector = GenerateData<float>(0);
        MPI_Barrier(MPI_COMM_WORLD);
        startTime = std::chrono::system_clock::now();
        for (uint64_t i = mpiRank; i < steps; i+=mpiSize)
        {
            dataManWriter.BeginStep();
            if(generateDataForEveryStep)
            {
                floatVector = GenerateData<float>(i);
            }
            dataManWriter.Put(floatArrayVar, floatVector.data());
            dataManWriter.Put(stepVar, i);
            PrintData(floatVector, dataManWriter.CurrentStep(), i, false);
            dataManWriter.EndStep();
        }
        dataManWriter.Close();
    }
    MPI_Barrier(MPI_COMM_WORLD);
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    size_t totalDataBytes = std::accumulate(shape.begin(), shape.end(), sizeof(float) * steps, std::multiplies<size_t>());
    float dataRate = (double)totalDataBytes / (double)duration.count();

    if(mpiRank == 0)
    {
        std::cout << "data rate = " << dataRate << " MB/s" << std::endl;
    }


    MPI_Finalize();

    return 0;
}
