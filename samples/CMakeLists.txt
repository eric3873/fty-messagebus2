project(${PROJECT_NAME}-sample
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty messagebus sample"
        LANGUAGES CXX)

# Dto(s)
add_subdirectory(dto)

############################### Addons samples #################################

################################################################################

# AMQP samples
if(BUILD_AMQP)
  add_subdirectory(amqp)
endif()

# Malamute samples
if(BUILD_MALAMUTE)
  add_subdirectory(malamute)
endif()

# Mqtt samples
if(BUILD_MQTT)
  add_subdirectory(mqtt)
endif()
