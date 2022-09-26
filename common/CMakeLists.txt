project(fty-messagebus2
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty messagebus 2 common interface"
)

##############################################################################################################
find_package(fty-cmake PATHS ${CMAKE_BINARY_DIR}/fty-cmake REQUIRED)
##############################################################################################################

etn_target(shared fty-messagebus2
    SOURCES
        src/*.cpp
    PUBLIC_INCLUDE_DIR
        public_include
    PUBLIC_HEADERS
        *.h
    USES_PUBLIC
        fty-utils
    USES_PRIVATE
        uuid
        pthread
)

set_target_properties(fty-messagebus2 PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR})

## Tests
etn_test_target(${PROJECT_NAME}
    SOURCES
        tests/*.cpp
)
