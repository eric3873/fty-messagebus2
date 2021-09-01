# Test target
etn_test_target(${PROJECT_NAME_UNDERSCORE}
  INCLUDE_DIRS
    private_include
  SOURCES
    src/UniTests.cpp
  USES
    ${PROJECT_NAME_UNDERSCORE}
)

if (EXTERNAL_SERVER_FOR_TEST)
  target_compile_definitions(${PROJECT_NAME_UNDERSCORE}-test PRIVATE EXTERNAL_SERVER_FOR_TEST)
endif()


if(BUILD_AMQP)
  target_sources(${PROJECT_NAME_UNDERSCORE}-test PRIVATE src/MsgBusAmqp.cpp)
endif()

if(BUILD_MALAMUTE)
  # target_sources(${PROJECT_NAME_UNDERSCORE}-test PRIVATE src/MsgBusMalamute.cpp)
endif()

if(BUILD_MQTT)
  #target_sources(${PROJECT_NAME_UNDERSCORE}-test PRIVATE src/MsgBusMqtt.cpp)
endif()
