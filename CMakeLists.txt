cmake_minimum_required(VERSION 2.8.0)
project(heliumdb CXX)

# compile options
option(DEBUG "Enable debug build" OFF)
option(TESTS "Enable unittests" OFF)
set(PYTHON_CONFIG "python3-config" CACHE STRING "python-config executable build with")
option(CDR "Enable cdr support" OFF)

if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set (CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/install" CACHE PATH "default install path" FORCE )
endif()

add_compile_options(-Wall)
if (DEBUG)
  message(STATUS "heliumdb DEBUG: ON")
  add_compile_options(-g3)
else ()
  message(STATUS "heliumdb DEBUG: OFF")
  add_compile_options(-g -O3)
endif (DEBUG)

# allow external projects
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

#deps
if(NOT SUBMODULE_ROOT_DIR)
    set(SUBMODULE_ROOT_DIR ${PROJECT_SOURCE_DIR})
endif()
if(NOT SUBMODULE_PREFIX_DIR)
    set(SUBMODULE_PREFIX_DIR ${CMAKE_BINARY_DIR})
endif()

link_directories(
    ${CMAKE_INSTALL_PREFIX}/lib
    )

#cdr
set(CDR_CMAKE ${SUBMODULE_ROOT_DIR}/ext/cdr CACHE PATH "Location of cdr-cmake" FORCE)
message(STATUS "cdr-location: " ${CDR_CMAKE})
ExternalProject_Add(
  CDR_PROJECT
  PREFIX ${SUBMODULE_PREFIX_DIR}
  SOURCE_DIR ${CDR_CMAKE}
  CMAKE_ARGS -DDEBUG=on -DPYTHON=on -DPYTHON_CONFIG=${PYTHON_CONFIG} -DSUBMODULE_PREFIX_DIR=${SUBMODULE_PREFIX_DIR} -DSUBMODULE_ROOT_DIR=${SUBMODULE_ROOT_DIR} -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} ${SUBMODULE_FLAGS}
  )
ExternalProject_Get_Property(CDR_PROJECT binary_dir)
set(CDR_PROJECT_DIR ${binary_dir}/lib/cmake/cdr CACHE PATH "location of cdr cmake files" FORCE)
set(CPACK_INSTALL_CMAKE_PROJECTS ${CPACK_INSTALL_CMAKE_PROJECTS} "${binary_dir};cdr;ALL;/")

include_directories(${CMAKE_INSTALL_PREFIX}/include)
include_directories(${CMAKE_INSTALL_PREFIX}/include/cdr)

include(${CMAKE_ROOT}/Modules/CheckIncludeFileCXX.cmake)

execute_process(
    COMMAND bash -c "${PYTHON_CONFIG} --includes"
    OUTPUT_VARIABLE PYTHON_INCLUDE_PATH
    RESULT_VARIABLE RET
    )

if(NOT ${RET} EQUAL "0")
    message(FATAL_ERROR "failed to run python-config")
endif()

execute_process(
    COMMAND bash -c "${PYTHON_CONFIG} --ldflags"
    OUTPUT_VARIABLE PYTHON_LDFLAGS
    RESULT_VARIABLE RET
    )

if(NOT ${RET} EQUAL "0")
    message(FATAL_ERROR "failed to run python-config")
endif()

string(REGEX REPLACE "\n$" "" PYTHON_LDFLAGS "${PYTHON_LDFLAGS}")
string(REGEX REPLACE "\n$" "" PYTHON_INCLUDE_PATH "${PYTHON_INCLUDE_PATH}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PYTHON_INCLUDE_PATH}")
link_libraries(${PYTHON_LDFLAGS})

set(INCLUDE_PATHS
    ${PROJECT_SOURCE_DIR}/src/helium/
    ${CMAKE_INSTALL_PREFIX}/include
)

find_library(LIB_HE
             NAMES he
             PATHS /usr /usr/local
             )
if(NOT LIB_HE)
    message(FATAL_ERROR "Failed to find libhe")
else()
    message(STATUS "Found libhe: ${LIB_HE}")
endif(NOT LIB_HE)

find_path(HE_H
          NAMES he.h
          PATHS /usr /usr/local
          )
if(NOT HE_H)
    message(FATAL_ERROR "failed to find he.h")
else()
    message(STATUS "Found he.h: ${HE_H}")
    include_directories(${HE_H})
endif(NOT HE_H)

SET(CMAKE_INSTALL_RPATH "/usr/lib")

configure_file(
  ${PROJECT_SOURCE_DIR}/${PROJECT_NAME}.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config-install.cmake
  @ONLY
  )

find_library (LIBHE
              NAMES he
              PATHS /usr /usr/local)
if(NOT LIBHE)
    message(FATAL_ERROR "failed to find libhe")
else()
    message(STATUS "found libhe: ${LIBHE}")
endif()

find_path(HE_H
          NAMES he.h
          PATHS /usr /usr/local
          )
if(NOT HE_H)
    message(FATAL_ERROR "failed to find he.h")
else()
    message(STATUS "found he.h: ${HE_H}")
    include_directories(${HE_H})
endif(NOT HE_H)

# add source
add_subdirectory(src)

if(TESTS)
  include(CTest)
  enable_testing()
  
  # add test dir if enabled
  add_subdirectory(test)
endif()
