#pragma once

#include "Python.h"
#include "bson.h"
#include <math.h>

#define EPSILON 0.000001

bool bson_query (bson_t* doc, bson_t* query);

bson_t* dictToBson (PyObject* d);
