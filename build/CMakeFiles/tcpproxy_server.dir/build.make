# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
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
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/y-watanabe/proxy-transaction-provider

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/y-watanabe/proxy-transaction-provider/build

# Include any dependencies generated for this target.
include CMakeFiles/tcpproxy_server.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/tcpproxy_server.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/tcpproxy_server.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/tcpproxy_server.dir/flags.make

CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o: CMakeFiles/tcpproxy_server.dir/flags.make
CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o: ../tcpproxy_server.cpp
CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o: CMakeFiles/tcpproxy_server.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/y-watanabe/proxy-transaction-provider/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o -MF CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o.d -o CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o -c /home/y-watanabe/proxy-transaction-provider/tcpproxy_server.cpp

CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.i"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/y-watanabe/proxy-transaction-provider/tcpproxy_server.cpp > CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.i

CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.s"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/y-watanabe/proxy-transaction-provider/tcpproxy_server.cpp -o CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.s

CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o: CMakeFiles/tcpproxy_server.dir/flags.make
CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o: ../acceptor.cpp
CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o: CMakeFiles/tcpproxy_server.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/y-watanabe/proxy-transaction-provider/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o -MF CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o.d -o CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o -c /home/y-watanabe/proxy-transaction-provider/acceptor.cpp

CMakeFiles/tcpproxy_server.dir/acceptor.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/tcpproxy_server.dir/acceptor.cpp.i"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/y-watanabe/proxy-transaction-provider/acceptor.cpp > CMakeFiles/tcpproxy_server.dir/acceptor.cpp.i

CMakeFiles/tcpproxy_server.dir/acceptor.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/tcpproxy_server.dir/acceptor.cpp.s"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/y-watanabe/proxy-transaction-provider/acceptor.cpp -o CMakeFiles/tcpproxy_server.dir/acceptor.cpp.s

CMakeFiles/tcpproxy_server.dir/bridge.cpp.o: CMakeFiles/tcpproxy_server.dir/flags.make
CMakeFiles/tcpproxy_server.dir/bridge.cpp.o: ../bridge.cpp
CMakeFiles/tcpproxy_server.dir/bridge.cpp.o: CMakeFiles/tcpproxy_server.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/y-watanabe/proxy-transaction-provider/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object CMakeFiles/tcpproxy_server.dir/bridge.cpp.o"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/tcpproxy_server.dir/bridge.cpp.o -MF CMakeFiles/tcpproxy_server.dir/bridge.cpp.o.d -o CMakeFiles/tcpproxy_server.dir/bridge.cpp.o -c /home/y-watanabe/proxy-transaction-provider/bridge.cpp

CMakeFiles/tcpproxy_server.dir/bridge.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/tcpproxy_server.dir/bridge.cpp.i"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/y-watanabe/proxy-transaction-provider/bridge.cpp > CMakeFiles/tcpproxy_server.dir/bridge.cpp.i

CMakeFiles/tcpproxy_server.dir/bridge.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/tcpproxy_server.dir/bridge.cpp.s"
	g++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/y-watanabe/proxy-transaction-provider/bridge.cpp -o CMakeFiles/tcpproxy_server.dir/bridge.cpp.s

# Object files for target tcpproxy_server
tcpproxy_server_OBJECTS = \
"CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o" \
"CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o" \
"CMakeFiles/tcpproxy_server.dir/bridge.cpp.o"

# External object files for target tcpproxy_server
tcpproxy_server_EXTERNAL_OBJECTS =

bin/tcpproxy_server: CMakeFiles/tcpproxy_server.dir/tcpproxy_server.cpp.o
bin/tcpproxy_server: CMakeFiles/tcpproxy_server.dir/acceptor.cpp.o
bin/tcpproxy_server: CMakeFiles/tcpproxy_server.dir/bridge.cpp.o
bin/tcpproxy_server: CMakeFiles/tcpproxy_server.dir/build.make
bin/tcpproxy_server: CMakeFiles/tcpproxy_server.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/y-watanabe/proxy-transaction-provider/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Linking CXX executable bin/tcpproxy_server"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/tcpproxy_server.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/tcpproxy_server.dir/build: bin/tcpproxy_server
.PHONY : CMakeFiles/tcpproxy_server.dir/build

CMakeFiles/tcpproxy_server.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/tcpproxy_server.dir/cmake_clean.cmake
.PHONY : CMakeFiles/tcpproxy_server.dir/clean

CMakeFiles/tcpproxy_server.dir/depend:
	cd /home/y-watanabe/proxy-transaction-provider/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/y-watanabe/proxy-transaction-provider /home/y-watanabe/proxy-transaction-provider /home/y-watanabe/proxy-transaction-provider/build /home/y-watanabe/proxy-transaction-provider/build /home/y-watanabe/proxy-transaction-provider/build/CMakeFiles/tcpproxy_server.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/tcpproxy_server.dir/depend

