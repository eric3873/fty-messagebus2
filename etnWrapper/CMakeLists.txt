project(etn-common-messagebus2
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "Eaton messagebus2"
)

etn_target(shared ${PROJECT_NAME} PUBLIC
  SOURCES
    src/*.cpp
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    *.h
  USES_PUBLIC
    fty-common-messagebus2
  USES_PRIVATE
    fty-common-messagebus2-amqp
    fty-common-messagebus2-mqtt
    fty_common_logging
)

set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR})

## Tests
if(BUILD_TESTING)
  etn_test_target(${PROJECT_NAME}
    SOURCES
      tests/*.cpp
  )
endif()
