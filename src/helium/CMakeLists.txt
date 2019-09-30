set (SOURCES
     module.cpp
     db/db.cpp
     db/iter.cpp
     ts/ts.cpp
     utils/he_utils.cpp
     utils/utils.cpp
     utils/cdr_utils.cpp
    )

include_directories(
    ${PROJECT_SOURCE_DIR}/src/helium
    ${PROJECT_SOURCE_DIR}/src/helium/db
    ${PROJECT_SOURCE_DIR}/src/helium/ts
    ${PROJECT_SOURCE_DIR}/src/helium/utils
  )

set (LINK_LIBS ${LIB_HE} cdr cdrpy)

execute_process(
    COMMAND bash -c "${PYTHON_CONFIG} --includes"
    OUTPUT_VARIABLE PYTHON_INCLUDE_PATH
    RESULT_VARIABLE RET
    )
if(NOT ${RET} EQUAL "0")
    message(FATAL_ERROR "failed to run python-config --includes")
endif()

execute_process(
    COMMAND bash -c "${PYTHON_CONFIG} --ldflags"
    OUTPUT_VARIABLE PYTHON_LDFLAGS
    RESULT_VARIABLE RET
    )
if(NOT ${RET} EQUAL "0")
    message(FATAL_ERROR "failed to run python-config --ldflags")
endif()

execute_process(
    COMMAND bash -c "${PYTHON_CONFIG} --libs"
    OUTPUT_VARIABLE PYTHON_LIBS
    RESULT_VARIABLE RET
    )
if(NOT ${RET} EQUAL "0")
    message(FATAL_ERROR "failed to run python-config --libs")
endif()

string(STRIP ${PYTHON_LDFLAGS} PYTHON_LDFLAGS)
string(STRIP ${PYTHON_INCLUDE_PATH} PYTHON_INCLUDE_PATH)
string(STRIP ${PYTHON_LIBS} PYTHON_LIBS)

message(STATUS "Python includes: ${PYTHON_INCLUDE_PATH}")
message(STATUS "Python ldflags: ${PYTHON_LDFLAGS}")
message(STATUS "Python libs: ${PYTHON_LIBS}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PYTHON_INCLUDE_PATH}")
link_libraries(${PYTHON_LDFLAGS})

add_library (helium SHARED ${SOURCES})
target_link_libraries(helium ${LINK_LIBS})
set_target_properties (helium PROPERTIES PREFIX "")
set_target_properties (helium PROPERTIES SUFFIX ".so")

install (TARGETS helium DESTINATION lib/python)
