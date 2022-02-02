project(${PROJECT_NAME}-sample
    VERSION ${PROJECT_VERSION}
    DESCRIPTION "fty messagebus sample"
    LANGUAGES CXX
)

# Dto(s)
add_subdirectory(dto)

############################### Addons samples #################################

################################################################################

# AMQP samples
if(BUILD_AMQP)
    add_subdirectory(amqp)
endif()

# Mqtt samples
if(BUILD_MQTT)
    add_subdirectory(mqtt)
endif()
