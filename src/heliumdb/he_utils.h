#pragma once

#include "Python.h"
#include "he.h"
#include "module.h"

using namespace std;

bool
he_utils_update (he_t& he, he_item& item)
{
    char err[128];
    int rc = 0;

    Py_BEGIN_ALLOW_THREADS
    rc = he_update (he, &item);
    Py_END_ALLOW_THREADS

    if (rc)
    {
        snprintf (err, 128, "he_update failed: %s", he_strerror (errno));
        PyErr_SetString (HeliumDbException, err);
        return false;
    }
    return true;
}

bool
he_utils_exists (he_t& he, he_item& item)
{
    int rc;
    Py_BEGIN_ALLOW_THREADS
    rc = he_exists (he, &item);
    Py_END_ALLOW_THREADS

    return (rc == 0);
}
