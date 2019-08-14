#include "cdr_utils.h"

static PyObject* CDR_MODULE = NULL;
static PyObject* CDR_CLASS = NULL;

static swig_type_info* CDR_TYPE = NULL;

bool
cdr_check (PyObject* o)
{
    if (PyObject_IsInstance (o, CDR_CLASS) != 1)
        return false;

    return true;
}

bool
cdr_utils_initCdr ()
{
    if (CDR_MODULE == NULL &&
        (CDR_MODULE = PyImport_ImportModuleNoBlock ("cdr")) == NULL)
    {
        // PyErr_SetString (HeliumDbException, "failed to import cdr");
        return false;
    }

    if (CDR_TYPE == NULL)
    {
        CDR_TYPE = SWIG_TypeQuery ("_p_neueda__cdr");
        if (!CDR_TYPE)
            return NULL;
    }

    return true;
}

bool
cdr_utils_toCdr (PyObject* pycdr, cdr*& d)
{
    void* argp = 0;
    int res = 0;

    if (!cdr_utils_initCdr ())
        return false;

    res = SWIG_ConvertPtr (pycdr, &argp, CDR_TYPE, 0 | 0);
    if (!SWIG_IsOK (res))
        return false;

    d = (cdr*)argp;
    return true;
}

PyObject*
cdr_utils_toPythonObj (cdr* d)
{
    if (!cdr_utils_initCdr ())
        return NULL;

    PyObject* o = SWIG_NewPointerObj (d, CDR_TYPE, SWIG_POINTER_OWN);
    return o;
}

PyObject*
cdr_utils_deserializeCdr (void* buf, size_t len)
{
    cdr* d = new cdr();
    size_t used = 0;
    if (!d->deserialize ((const char*)buf, used))
        return NULL;

    return cdr_utils_toPythonObj (d);
}

bool
cdr_utils_query (cdr* d, cdr* query)
{
    for (cdr::iterator it = query->begin (); it != query->end (); ++it)
    {
        cdrItem a = it->second;
        if (!d->contains (a.mKey))
            return false;

        const cdrItem* b = d->getItem (a.mKey);
        if (a.mType != b->mType)
            return false;

        switch (a.mType)
        {
            case CDR_INTEGER:
                if (a.mInteger != b->mInteger)
                    return false;
                break;
            case CDR_STRING:
                if (a.mString != b->mString)
                    return false;
                break;
            case CDR_DOUBLE:
                if (!(fabs (a.mDouble - b->mDouble) < EPSILON))
                    return false;
                break;
            default:
                return false;
        }
    }

    return true;
}
