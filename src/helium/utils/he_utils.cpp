#include "he_utils.h"

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

bool
he_utils_retrieve (he_t& he, he_item& getItem)
{
    size_t  rdSize = getItem.val_len;
    void*   buf = NULL;

    int rc;
    for (;;)
    {
		Py_BEGIN_ALLOW_THREADS
        rc = he_lookup (he, &getItem, 0, rdSize);
        Py_END_ALLOW_THREADS

        if (rc != 0)
        {
            // PyErr_SetString (HeliumDbException, "he_lookup failed");
            return false;
        }

        if (getItem.val_len > rdSize)
        {
            rdSize = getItem.val_len;
            buf = malloc (rdSize);
            getItem.val = buf;
        }
        else
            break;
    }
    return true;
}
