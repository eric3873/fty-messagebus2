project(${SAMPLE_DTO_LIB_NAME}
        VERSION ${PROJECT_VERSION}
        DESCRIPTION "fty common message bus sample dto shared with mlm and mqtt samples"
        LANGUAGES CXX)

##############################################################################################################
find_package(nlohmann_json 3.2.0 REQUIRED)
##############################################################################################################

etn_target(shared ${PROJECT_NAME} PRIVATE
  PUBLIC_INCLUDE_DIR
    public_include
  PUBLIC_HEADERS
    *.hpp
  SOURCES
    src/*.cpp
  USES_PUBLIC
    fty-messagebus2
    nlohmann_json::nlohmann_json
)
