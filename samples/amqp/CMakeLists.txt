project(${PROJECT_NAME}-amqp
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty messagebus sample for amqp"
        LANGUAGES CXX
)

set (THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Pub/Sub
etn_target(exe ${PROJECT_NAME}-pub-sub PRIVATE
  SOURCES
    src/*PubSub.cpp
  USES_PRIVATE
    fty-common-messagebus2-amqp
    fty_common_logging
    ${SAMPLE_DTO_LIB_NAME}
  FLAGS
    "-fmax-errors=1"
)

# Math sample replyer
etn_target(exe ${PROJECT_NAME}-async-reply PRIVATE
  SOURCES
    src/*SampleAsyncReply.cpp
  USES_PUBLIC
    fty-common-messagebus2-amqp
    fty_common_logging
    ${SAMPLE_DTO_LIB_NAME}
  FLAGS
    "-fmax-errors=1"
)

# Math sample asynchrone and synchrone requester
etn_target(exe ${PROJECT_NAME}-send-request PRIVATE
  SOURCES
    src/*SampleSendRequest.cpp
  USES_PUBLIC
    fty-common-messagebus2-amqp
    fty_common_logging
    ${SAMPLE_DTO_LIB_NAME}
  FLAGS
    "-fmax-errors=1"
)
