
project(fty_common_messagebus2_amqp
  VERSION ${PROJECT_VERSION}
  DESCRIPTION "fty messagebus2 amqp library"
)

##############################################################################################################
find_package(ProtonCpp REQUIRED)
##############################################################################################################

etn_target(shared ${PROJECT_NAME} PUBLIC
  INCLUDE_DIRS
    private_include
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    *.hpp
  SOURCES
    src/*.cpp
  USES_PUBLIC
    fty_common_messagebus_utils
    fty-messagebus-common
    fty_common_logging
    Proton::cpp
  FLAGS
    "-fmax-errors=2"
)

set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR})
