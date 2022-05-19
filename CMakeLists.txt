cmake_minimum_required(VERSION 3.13)
cmake_policy(VERSION 3.13)

project(fty-messagebus2
    VERSION 1.0.0
    DESCRIPTION "fty common messagebus2"
)

##############################################################################################################
find_package(fty-cmake PATHS ${CMAKE_BINARY_DIR}/fty-cmake REQUIRED)
##############################################################################################################

## TO BE MOVED IN FTY-CMAKE
macro(relative_option parent_option new_option_name text)
  if (${parent_option})
    option(${new_option_name} ${text} ON)
  else()
    option(${new_option_name} ${text} OFF)
  endif()
endmacro()

option(                            BUILD_ALL                   "Build all addons"                  ON)
relative_option(${BUILD_ALL}       BUILD_AMQP                  "Build AMQP addon"                    )
relative_option(${BUILD_ALL}       BUILD_MQTT                  "Build MQTT addon"                    )
option(                            BUILD_SAMPLES               "Build samples"                     ON)
option(                            BUILD_DOC                   "Build documentation"               OFF)
option(                            EXTERNAL_SERVER_FOR_TEST    "Using external server for test"    OFF)

# Library
add_subdirectory(utils)
add_subdirectory(common)

# Addons

if(BUILD_AMQP)
  add_subdirectory(amqp)
endif()

if(BUILD_MQTT)
  add_subdirectory(mqtt)
endif()

# Samples
if(BUILD_SAMPLES)
  set(SAMPLE_DTO_LIB_NAME fty-messagebus2-sample-dto)
  add_subdirectory(samples)
endif()
