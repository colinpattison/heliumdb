install(DIRECTORY python/ DESTINATION bin/test/python
        FILES_MATCHING PATTERN "*.py")
add_test(NAME pyunittest
    COMMAND ${CMAKE_COMMAND} -E env PYTHONPATH=${CMAKE_INSTALL_PREFIX}/lib/python
    ${PYTHON_EXECUTABLE} -m unittest discover ${CMAKE_INSTALL_PREFIX}/bin/test/python
)
