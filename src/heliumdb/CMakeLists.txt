set (SOURCES
     module.cpp
     utils.cpp
     iter.cpp
    )

set (LINK_LIBS he)

if (BSON)
    set (LINK_LIBS ${LINK_LIBS} bson-1.0)
    set (SOURCES ${SOURCES} bson_utils.cpp)
endif(BSON)

add_library (heliumdb SHARED ${SOURCES})
target_link_libraries(heliumdb ${LINK_LIBS})
set_target_properties (heliumdb PROPERTIES PREFIX "")
set_target_properties (heliumdb PROPERTIES SUFFIX ".so")

install (TARGETS heliumdb DESTINATION lib/python)
