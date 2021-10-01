
project(fty-common-messagebus2-mqtt
  VERSION 1.0.0
  DESCRIPTION "fty messagebus2 mqtt library"
)

##############################################################################################################
find_package(PahoMqttCpp REQUIRED)
##############################################################################################################

etn_target(shared ${PROJECT_NAME} PUBLIC
  SOURCES
    src/*.cpp
    src/*.h
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    fty/messagebus/mqtt/MessageBusMqtt.h
  USES_PUBLIC
    fty-common-messagebus2
  USES_PRIVATE
    fty-common-messagebus-utils
    fty_common_logging
    paho-mqttpp3
    paho-mqtt3as
    paho-mqtt3c
  FLAGS
    "-fmax-errors=1"
)

set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR})

## Tests
if(BUILD_TESTING)
  etn_test_target(${PROJECT_NAME}
    SOURCES
      tests/*.cpp
  )
endif()
