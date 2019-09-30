#include "module.h"
#include <utility>
#include <sstream>
#include <iostream>

using namespace std;

struct module_state 
{
    PyObject* error;
};

#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

#define DEFERRED_ADDRESS(ADDR) 0

#if PY_MAJOR_VERSION >= 3
static int helium_traverse (PyObject *m, visitproc visit, void *arg) 
{
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}

static int helium_clear (PyObject *m) 
{
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}

static PyMethodDef helium_methods[] = {
    { NULL, NULL, 0, NULL }
};

static struct PyModuleDef moduledef = 
{
        PyModuleDef_HEAD_INIT,
        "helium",
        NULL,
        sizeof(struct module_state),
        helium_methods,
        NULL,
        helium_traverse,
        helium_clear,
        NULL
};

#define INITERROR return NULL

PyMODINIT_FUNC
PyInit_helium (void)
#else
#define INITERROR return

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
inithelium (void)
#endif
{
    PyObject* m = NULL;

    PyEval_InitThreads ();

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create (&moduledef);
#else
    m = Py_InitModule ("helium", helium_methods);
#endif

    if (m == NULL)
#if PY_MAJOR_VERSION >= 3
        return NULL;
#else
        return;
#endif

    // adding db class to module
    heliumdbPyType.tp_new = PyType_GenericNew;
    if(PyType_Ready (&heliumdbPyType) < 0)
    #if PY_MAJOR_VERSION >= 3
            return NULL;
    #else
            return;
    #endif
    Py_INCREF (&heliumdbPyType);
    PyModule_AddObject (m, "Db", (PyObject*)&heliumdbPyType);

    // adding exception
    HeliumDbException = PyErr_NewException ("helium.HeliumException", NULL, NULL);
    Py_INCREF (HeliumDbException);
    PyModule_AddObject (m, "HeliumException", HeliumDbException);

    // adding ts class to module
    heliumtsPyType.tp_new = PyType_GenericNew;
    if(PyType_Ready (&heliumtsPyType) < 0)
    #if PY_MAJOR_VERSION >= 3
            return NULL;
    #else
            return;
    #endif
    Py_INCREF (&heliumtsPyType);
    PyModule_AddObject (m, "Ts", (PyObject*)&heliumtsPyType);

    PyModule_AddIntConstant (m, "HE_O_CREATE", 1);
    PyModule_AddIntConstant (m, "HE_O_TRUNCATE", 2);
    PyModule_AddIntConstant (m, "HE_O_VOLUME_CREATE", 4);
    PyModule_AddIntConstant (m, "HE_O_VOLUME_TRUNCATE", 8);
    PyModule_AddIntConstant (m, "HE_O_VOLUME_NOTRIM", 16);
    PyModule_AddIntConstant (m, "HE_O_NOSORT", 32);
    PyModule_AddIntConstant (m, "HE_O_SCAN", 64);
    PyModule_AddIntConstant (m, "HE_O_CLEAN", 128);
    PyModule_AddIntConstant (m, "HE_O_COMPRESS", 256);
    PyModule_AddIntConstant (m, "HE_O_READONLY", 512);
    PyModule_AddIntConstant (m, "HE_O_ERR_EXISTS", 1024);

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
