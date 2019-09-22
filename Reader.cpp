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
#include <mpi.h>
#include <numeric>
#include <thread>
#include <vector>

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

int main(int argc, char *argv[])
{
    // initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpiRank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpiSize);

    int port = 12306 + mpiRank*2;

    // initialize adios2
    adios2::ADIOS adios;
    adios2::IO dataManIO = adios.DeclareIO("whatever");
    dataManIO.SetEngine("DataMan");
    dataManIO.SetParameters({{"IPAddress", "127.0.0.1"}, {"Port", std::to_string(port)}, {"Timeout", "5"}});

    // open stream
    adios2::Engine dataManReader = dataManIO.Open("HelloDataMan", adios2::Mode::Read);

    // read data
    std::vector<float> floatVector;
    MPI_Barrier(MPI_COMM_WORLD);
    auto startTime = std::chrono::system_clock::now();
    size_t steps = 0;
    adios2::Dims shape;
    while (true)
    {
        auto status = dataManReader.BeginStep();
        if (status == adios2::StepStatus::OK)
        {
            ++steps;

            // get variable FloatArray
            auto floatArrayVar = dataManIO.InquireVariable<float>("FloatArray");
            shape = floatArrayVar.Shape();
            size_t datasize = std::accumulate(shape.begin(), shape.end(), 1, std::multiplies<size_t>());
            floatVector.resize(datasize);
            dataManReader.Get<float>(floatArrayVar, floatVector.data());

            // get variable GlobalStep
            uint64_t globalStep;
            auto stepVar = dataManIO.InquireVariable<uint64_t>("GlobalStep");
            dataManReader.Get<uint64_t>(stepVar, globalStep);

            dataManReader.EndStep();
            PrintData(floatVector, dataManReader.CurrentStep(), globalStep, false);
        }
        else if (status == adios2::StepStatus::EndOfStream)
        {
            std::cout << "End of stream" << std::endl;
            break;
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    auto endTime = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    size_t rankTotalDataBytes = std::accumulate(shape.begin(), shape.end(), sizeof(float) * steps, std::multiplies<size_t>());
    size_t totalDataBytes;

    MPI_Reduce(&rankTotalDataBytes, &totalDataBytes, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    float dataRate = (double)totalDataBytes / (double)duration.count();
    if(mpiRank == 0)
    {
        std::cout << "data rate = " << dataRate << " MB/s" << std::endl;
    }

    // clean up
    dataManReader.Close();
    MPI_Finalize();

    return 0;
}
