project(etn-common-messagebus2
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "Eaton messagebus2"
)

etn_target(shared ${PROJECT_NAME} PUBLIC
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    *.h
  SOURCES
    src/*.cpp
  USES_PUBLIC
    fty-common-messagebus2
    fty-common-messagebus2-amqp
    fty-common-messagebus2-mqtt
    fty_common_logging
)

## Tests
if(BUILD_TESTING)
  etn_test_target(${PROJECT_NAME}
    SOURCES
      tests/*.cpp
  )
endif()
