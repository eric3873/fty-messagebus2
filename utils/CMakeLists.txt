project(fty-messagebus2-utils
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty messagebus utilitary static library"
)

etn_target(static ${PROJECT_NAME} PRIVATE
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    *.hpp
  SOURCES
    src/*.cpp
  USES_PUBLIC
    pthread
)

## Tests
if(BUILD_TESTING)
  etn_test_target(${PROJECT_NAME}
    SOURCES
      tests/*.cpp
  )
endif()
