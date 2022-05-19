project(${PROJECT_NAME}-mqtt
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty messagebus sample for mqtt"
        LANGUAGES CXX
)

add_subdirectory(publish)
add_subdirectory(daemon)

## Math sample replyer
etn_target(exe ${PROJECT_NAME}-async-reply PRIVATE
 SOURCES
   src/*SampleAsyncReply.cpp
  USES_PRIVATE
   fty-messagebus2-mqtt
   ${SAMPLE_DTO_LIB_NAME}
   fty_common_logging
 FLAGS
   "-fmax-errors=1"
)

## Math sample asynchrone and synchrone requester
etn_target(exe ${PROJECT_NAME}-send-request PRIVATE
 SOURCES
   src/*SampleSendRequest.cpp
 USES_PRIVATE
   fty-messagebus2-mqtt
   ${SAMPLE_DTO_LIB_NAME}
   fty_common_logging
 FLAGS
   "-fmax-errors=1"
)

## Discovery topic
etn_target(exe ${PROJECT_NAME}-discovery PRIVATE
 SOURCES
   src/*SampleDiscovery.cpp
 USES_PRIVATE
   fty-messagebus2-mqtt
   fty_common_logging
 FLAGS
   "-fmax-errors=1"
)
