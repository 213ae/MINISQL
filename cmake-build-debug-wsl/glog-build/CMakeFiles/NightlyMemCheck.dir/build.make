# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /mnt/d/Repos/minisql-master

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /mnt/d/Repos/minisql-master/cmake-build-debug-wsl

# Utility rule file for NightlyMemCheck.

# Include the progress variables for this target.
include glog-build/CMakeFiles/NightlyMemCheck.dir/progress.make

glog-build/CMakeFiles/NightlyMemCheck:
	cd /mnt/d/Repos/minisql-master/cmake-build-debug-wsl/glog-build && /usr/bin/ctest -D NightlyMemCheck

NightlyMemCheck: glog-build/CMakeFiles/NightlyMemCheck
NightlyMemCheck: glog-build/CMakeFiles/NightlyMemCheck.dir/build.make

.PHONY : NightlyMemCheck

# Rule to build all files generated by this target.
glog-build/CMakeFiles/NightlyMemCheck.dir/build: NightlyMemCheck

.PHONY : glog-build/CMakeFiles/NightlyMemCheck.dir/build

glog-build/CMakeFiles/NightlyMemCheck.dir/clean:
	cd /mnt/d/Repos/minisql-master/cmake-build-debug-wsl/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/NightlyMemCheck.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/NightlyMemCheck.dir/clean

glog-build/CMakeFiles/NightlyMemCheck.dir/depend:
	cd /mnt/d/Repos/minisql-master/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /mnt/d/Repos/minisql-master /mnt/d/Repos/minisql-master/thirdparty/glog /mnt/d/Repos/minisql-master/cmake-build-debug-wsl /mnt/d/Repos/minisql-master/cmake-build-debug-wsl/glog-build /mnt/d/Repos/minisql-master/cmake-build-debug-wsl/glog-build/CMakeFiles/NightlyMemCheck.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/NightlyMemCheck.dir/depend
