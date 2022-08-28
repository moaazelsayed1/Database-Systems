[![Build Status](https://github.com/cmu-db/bustub/actions/workflows/cmake.yml/badge.svg)](https://github.com/cmu-db/bustub/actions/workflows/cmake.yml)

BusTub is a relational database management system built at [Carnegie Mellon University](https://db.cs.cmu.edu) for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. This system was developed for educational purposes and should not be used in production environments.

## Projects

- [x] [C++ Primer](https://15445.courses.cs.cmu.edu/fall2021/project0/) ([auto-graded result](./results/primer.png))
- [x] [Buffer Pool Manager](https://15445.courses.cs.cmu.edu/fall2021/project1/) ([auto graded result](./results/bufferPool.png)) ([Leaderboard](./results/bufferPoolBoard.png))
- [x] [Hash Index](https://15445.courses.cs.cmu.edu/fall2021/project2/) ([auto-graded result](./results/hashIndex.png)) ([Leaderboard](./results/hashIndexBoard.png))
- [ ] [Query Execution](https://15445.courses.cs.cmu.edu/fall2021/project3/)
- [ ] [Concurrency Control](https://15445.courses.cs.cmu.edu/fall2021/project4/)

## Build

### Linux / Mac

To ensure that you have the proper packages on your machine, run the following script to automatically install them:

```
$ sudo build_support/packages.sh
```

Then run the following commands to build the system:

```
$ mkdir build
$ cd build
$ cmake ..
$ make
```

If you want to compile the system in debug mode, pass in the following flag to cmake:
Debug mode:

```
$ cmake -DCMAKE_BUILD_TYPE=Debug ..
$ make
```

This enables [AddressSanitizer](https://github.com/google/sanitizers), which can generate false positives for overflow on STL containers. If you encounter this, define the environment variable `ASAN_OPTIONS=detect_container_overflow=0`.

### Windows

If you are using Windows 10, you can use the Windows Subsystem for Linux (WSL) to develop, build, and test Bustub. All you need is to [Install WSL](https://docs.microsoft.com/en-us/windows/wsl/install-win10). You can just choose "Ubuntu" (no specific version) in Microsoft Store. Then, enter WSL and follow the above instructions.

If you are using CLion, it also [works with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends).

## Testing

```
$ cd build
$ make check-tests
```
