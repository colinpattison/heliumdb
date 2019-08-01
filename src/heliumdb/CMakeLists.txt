set (SOURCES
     module.cpp
     utils.cpp
     iter.cpp
    )

set (LINK_LIBS he)

if (CDR)
    set (LINK_LIBS ${LINK_LIBS} cdr)
    # set (SOURCES ${SOURCES} cdr_utils.cpp)
endif(CDR)

add_library (heliumdb SHARED ${SOURCES})
target_link_libraries(heliumdb ${LINK_LIBS})
set_target_properties (heliumdb PROPERTIES PREFIX "")
set_target_properties (heliumdb PROPERTIES SUFFIX ".so")

install (TARGETS heliumdb DESTINATION lib/python)
