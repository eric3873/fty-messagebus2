project(fty_common_messagebus_utils
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty messagebus utilitary static library"
)

find_package(Threads REQUIRED)

etn_target(static ${PROJECT_NAME} PRIVATE
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    *.hpp
  SOURCES
    src/*.cpp
  USES_PUBLIC
    Threads::Threads
)
