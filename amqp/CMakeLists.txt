
project(fty-common-messagebus2-amqp
  VERSION 1.0.0
  DESCRIPTION "fty messagebus2 amqp library"
)

##############################################################################################################
find_package(ProtonCpp REQUIRED)
##############################################################################################################

etn_target(shared ${PROJECT_NAME} PUBLIC
  SOURCES
    src/*.cpp
    src/*.h
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    fty/messagebus/amqp/MessageBusAmqp.h
  USES_PUBLIC
    fty-common-messagebus2
  USES_PRIVATE
    fty_common_messagebus_utils
    fty_common_logging
    Proton::cpp
  FLAGS
    "-fmax-errors=2"
    #"-w"
)

set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR})

## Tests
if(BUILD_TESTING)
  etn_test_target(${PROJECT_NAME}
    SOURCES
      tests/*.cpp
  )
endif()
