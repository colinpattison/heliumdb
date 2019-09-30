#pragma once

#include <Python.h>
#include "cdr.h"
#include "swigPyRuntime.h"
#include "cdrPy.h"

using namespace neueda;

#define EPSILON 0.000001

bool cdr_utils_query (cdr* d, cdr* query);

PyObject* cdr_utils_deserializeCdr (void* buf, size_t len);

bool cdr_utils_serializeCdr (PyObject* o, void*& v, size_t& l);
