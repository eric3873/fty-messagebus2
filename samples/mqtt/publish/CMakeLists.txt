project(${PROJECT_NAME}-publish
        VERSION 1.0.0
        DESCRIPTION "fty messagebus sample for mqtt publish"
        LANGUAGES CXX
)

##############################################################################################################
find_package(fty-cmake PATHS ${CMAKE_BINARY_DIR}/fty-cmake REQUIRED)
##############################################################################################################

# Pub/Sub
etn_target(exe ${PROJECT_NAME} PRIVATE
  SOURCES
    *.cpp
  USES_PRIVATE
    fty-common-messagebus2-mqtt
    fty_common_logging
)
