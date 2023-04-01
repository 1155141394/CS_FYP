cmake_minimum_required(VERSION 3.5)
project(query)
find_package(AWSSDK REQUIRED COMPONENTS s3crt)
set(CMAKE_CXX_STANDARD 11)
add_executable(query "query.cpp")
# list all deps for static linking
target_link_libraries(query ${AWSSDK_LINK_LIBRARIES})