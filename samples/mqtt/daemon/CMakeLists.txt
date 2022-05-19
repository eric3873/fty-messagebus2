project(${PROJECT_NAME}-daemon-basic
        VERSION 1.0.0
        DESCRIPTION "fty messagebus sample for mqtt publish"
        LANGUAGES CXX
)

##############################################################################################################
find_package(fty-cmake PATHS ${CMAKE_BINARY_DIR}/fty-cmake REQUIRED)
##############################################################################################################

#daemon
etn_target(exe ${PROJECT_NAME} PRIVATE
  SOURCES
    daemon.cpp
  USES_PRIVATE
    fty-messagebus2-mqtt
    fty_common_logging
)

#client
etn_target(exe ${PROJECT_NAME}-cli PRIVATE
  SOURCES
    client.cpp
  USES_PRIVATE
    fty-messagebus2-mqtt
)
