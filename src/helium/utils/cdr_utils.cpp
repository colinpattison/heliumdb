#include "cdr_utils.h"

PyObject*
cdr_utils_deserializeCdr (void* buf, size_t len)
{
    cdr* d = new cdr();
    size_t used = 0;
    if (!d->deserialize ((const char*)buf, used))
        return NULL;

    return cdrToPyObj (d);
}

bool
cdr_utils_serializeCdr (PyObject* o, void*& v, size_t& l)
{
    cdr* d;
    if (!cdrFromPyObj (o, d))
        return false;

    char* space = new char[d->serializedSize ()];
    size_t used = 0;

    if (!d->serialize (space, used, true))
        return false;

    v = (void*)space;
    l = used;

    return true;
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
