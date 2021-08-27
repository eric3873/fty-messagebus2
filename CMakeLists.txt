cmake_minimum_required(VERSION 3.13)
cmake_policy(VERSION 3.13)

project(fty-common-messagebus2
    VERSION 1.0.0
    DESCRIPTION "fty common messagebus2"
)

##############################################################################################################
find_package(fty-cmake PATHS ${CMAKE_BINARY_DIR}/fty-cmake REQUIRED)
set (THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
##############################################################################################################

macro(relative_option parent_option new_option_name text)
  if (${parent_option})
    option(${new_option_name} ${text} ON)
  else()
    option(${new_option_name} ${text} OFF)
  endif()
endmacro()

option(                            BUILD_ALL                   "Build all addons"                  ON)
relative_option(${BUILD_ALL}       BUILD_AMQP                  "Build AMQP addon"                    )
relative_option(${BUILD_ALL}       BUILD_MALAMUTE              "Build Malamute addon"                )
relative_option(${BUILD_ALL}       BUILD_MQTT                  "Build MQTT addon"                    )

option(                            BUILD_SAMPLES               "Build samples"                    OFF)
option(                            BUILD_TESTING               "Build tests"                      OFF)
option(                            BUILD_DOC                   "Build documentation"              OFF)
option(                            EXTERNAL_SERVER_FOR_TEST    "Using external server for test"   OFF)

set(PROJECT_NAME_UNDERSCORE fty_common_messagebus2)
set(SAMPLE_DTO_LIB_NAME fty-common-messagebus2-sample-dto)

# Utils, interfaces, addons, etc.
add_subdirectory(utils)
add_subdirectory(common)
add_subdirectory(addons)

# Library
add_subdirectory(lib)

# Tools
if(BUILD_MALAMUTE)
  add_subdirectory(tools)
endif()

# Samples
if(BUILD_SAMPLES)
  add_subdirectory(samples)
endif()
