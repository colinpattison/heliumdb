#include "module.h"
#include "datetime.h"
#include <utility>
#include <sstream>
#include <iostream>
#include "he_utils.h"
#include "cdr_utils.h"

using namespace std;

static void
heliumtsPy_dealloc (heliumtsPy* self)
{
    Py_TYPE (self)->tp_free((PyObject*)self);
}

int
heliumtsPy_init (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    if (heliumdbPyType.tp_init((PyObject *)self, args, kwargs) < 0)
        return -1;

    return 0;
}

bool
_heliumUpdateCdr (he_t& he, he_item& item, cdr& d)
{
    char* space = new char[d.serializedSize ()];
    size_t used = 0;
    if (!d.serialize (space, used, true))
    {
        PyErr_SetString (HeliumDbException, "failed to serialize cdr");
        return false;
    }

    item.val = (void*)space;
    item.val_len = used;

    if (!he_utils_update (he, item))
        return false;

    delete[] space;

    return true;
}

bool
_heliumInsertCdr (he_t& he, he_item& item, cdr& d)
{
    cdr existing;

    if (!he_utils_exists (he, item))
    {
        // key doesn't exist
        if (!_heliumUpdateCdr (he, item, d))
        {
            PyErr_SetString (HeliumDbException, "update failed");
            return false;
        }
    }
    else
    {
        // key already exists
        char* buffer[8096] = {0};
        item.val = (void*)buffer;
        item.val_len = sizeof (buffer);
        if (!he_utils_retrieve (he, item))
        {
            PyErr_SetString (HeliumDbException, "retrieve failed");
            return false;
        }

        size_t used = 0;
        if (!existing.deserialize ((const char*)item.val, used))
        {
            PyErr_SetString (HeliumDbException, "failed to deserialize");
            return false;
        }

        cdrArray* entryArray;
        if (!d.getArray (0, (const cdrArray**)(&entryArray)))
            return false;

        for (cdrArray::iterator it = entryArray->begin();
             it != entryArray->end(); ++it)
            existing.appendArray (0, *it);

        if (!_heliumUpdateCdr (he, item, existing))
        {
            PyErr_SetString (HeliumDbException, "update failed");
            return false;
        }
    }

    return true;
}

PyObject*
heliumdb_insert_many (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    PyObject* data = Py_None;

    char *kwlist[] = {(char*)"data",
                      NULL};

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "|O",
                                     kwlist,
                                     &data))
        return NULL;

    if (!PyList_Check (data))
    {
        PyErr_SetString (HeliumDbException, "Unexpected type");
        return NULL;
    }

    int64_t         millis;
    int64_t         lastKey = 0;
    int64_t         key;
    he_item         item;
    cdr             entries;
    cdr             existing;
    cdr*            d;

    for (int i = 0; i < PyList_Size (data); i++)
    {
        PyObject* pycdr = PyList_GetItem (data, i);

        // if (!cdr_check (pycdr))
        //     return NULL;

        if (!cdrFromPyObj (pycdr, d))
        {
            PyErr_SetString (HeliumDbException, "failed to retrieve cdr object");
            return NULL;
        }

        // cdrDateTime t;
        // if (!d->getDateTime (self->mIndexField, t))
        // {
        //     PyErr_SetString (HeliumDbException, "failed to retrieve index field");
        //     return NULL;
        // }
        // millisFromMidnight (t, millis);
        if (!d->getInteger (self->mIndexField, millis))
        {
            PyErr_SetString (HeliumDbException, "failed to retrieve index field");
            return NULL;
        }

        // round down to nearest 10
        key = (millis / 10) * 10;

        if (lastKey == 0)
            lastKey = key;

        if (key != lastKey)
        {
            // new millisecond
            item.key = &lastKey;
            item.key_len = sizeof (lastKey);

            if (!_heliumInsertCdr (self->mDatastore, item, entries))
                return NULL;

            entries.clear ();
            lastKey = key;
        }

        entries.appendArray (0, *d);
    }

    item.key = &key;
    item.key_len = sizeof (key);

    if (!_heliumInsertCdr (self->mDatastore, item, entries))
        return NULL;

    Py_INCREF (Py_None);
    return Py_None;
}

PyObject*
heliumdb_insert_one (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    PyObject* data = Py_None;

    char *kwlist[] = {(char*)"data",
                      NULL};

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "|O",
                                     kwlist,
                                     &data))
        return NULL;

    cdr* d;
    if (!cdrFromPyObj (data, d))
    {
        PyErr_SetString (HeliumDbException, "failed to get cdr object");
        return NULL;
    }

    // cdrDateTime t;
    // if (!d->getDateTime (self->mIndexField, t))
    // {
    //     PyErr_SetString (HeliumDbException, "failed to retrieve index field");
    //     return NULL;
    // }

    int64_t millis = 0;
    if (!d->getInteger (self->mIndexField, millis))
    {
        PyErr_SetString (HeliumDbException, "failed to retrieve index field");
        return NULL;
    }
    // millisFromMidnight (t, millis);
    int64_t key = (millis / 10) * 10;

    he_item item;
    item.key = &key;
    item.key_len = sizeof (key);

    cdr entries;
    entries.appendArray (0, *d);
    if (!_heliumInsertCdr (self->mDatastore, item, entries))
    {
        PyErr_SetString (HeliumDbException, "failed to insert cdr");
        return NULL;
    }

    Py_INCREF (Py_None);
    return Py_None;
}

bool
compareResults (pair<int64_t, PyObject*> x, pair<int64_t, PyObject*> y)
{
    return (x.first < y.first);
}

PyObject*
_filterFind (heliumdbPy* self, PyObject* args, PyObject* kwargs, bool findOne)
{
    PyObject* qdict = Py_None;
    he_iter_t itr;

    char *kwlist[] = {(char*)"data",
                      NULL};

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "|O",
                                     kwlist,
                                     &qdict))
        return NULL;

    if (!PyDict_Check (qdict))
    {
        PyErr_SetString (HeliumDbException, "expected a dictionary");
        return NULL;
    }
    
    cdr* query = new cdr ();
    if (!cdrFromPyDict (qdict, query))
    {
        PyErr_SetString (HeliumDbException, "failed to convert to cdr");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    itr = he_iter_open (self->mDatastore, NULL, 0, HE_MAX_VAL_LEN, 0);
    Py_END_ALLOW_THREADS
    if (!itr)
    {
        PyErr_SetString (HeliumDbException, "failed to open iterator");
        return NULL;
    }

    const he_item* item;
    const cdrArray* entries;
    size_t used = 0;

    vector< pair<int64_t, PyObject*> > results;
    cdr* d = new cdr ();
    while ((item = he_iter_next (itr)))
    {
        used = 0;
        if (!d->deserialize ((const char*)item->val, used))
        {
            PyErr_SetString (HeliumDbException, "failed to deserialize cdr");
            return NULL;
        }

        if (!d->getArray (0, &entries))
            continue;

        for (cdrArray::const_iterator it = entries->begin (); it != entries->end (); ++it)
        {
            if (cdr_utils_query (const_cast<cdr*>(&(*it)), query))
            {
                if (findOne)
                    return cdrToPyObj (new cdr(*it));
                else
                {
                    results.push_back(make_pair (*(int64_t*)item->key,
                                                 cdrToPyObj (new cdr (*it))));
                }
            }
        }
    }
    delete d;

    if (findOne)
    {
        Py_INCREF (Py_None);
        return Py_None;
    }

    sort (results.begin (), results.end (), compareResults);

    PyObject* sorted_results = PyList_New (0);

    vector< pair<int64_t, PyObject*> >::iterator vitr;
    for (vitr = results.begin (); vitr != results.end (); vitr++)
        PyList_Append (sorted_results, vitr->second);

    return sorted_results;
}

PyObject*
heliumdb_find (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    return _filterFind (self, args, kwargs, false);
}

PyObject*
heliumdb_find_one (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    return _filterFind (self, args, kwargs, true);
}

PyObject*
_filterDelete (heliumdbPy* self, PyObject* args, PyObject* kwargs, bool deleteOne)
{
    he_iter_t       itr;
    const he_item*  item;
    int             rc;
    PyObject* qdict = Py_None;

    char *kwlist[] = {(char*)"data",
                      NULL};

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "|O",
                                     kwlist,
                                     &qdict))
        return NULL;

    if (!PyDict_Check (qdict))
        return NULL;

    cdr* query = new cdr ();
    if (!cdrFromPyDict (qdict, query))
    {
        PyErr_SetString (HeliumDbException, "failed to convert to cdr");
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS
    itr = he_iter_open (self->mDatastore, NULL, 0, HE_MAX_VAL_LEN, 0);
    Py_END_ALLOW_THREADS
    if (!itr)
    {
        PyErr_SetString (HeliumDbException, "failed to open iterator");
        return NULL;
    }

    size_t used;
    cdr* d = new cdr();
    const cdrArray* entries;
    while ((item = he_iter_next (itr)))
    {
        used = 0;
        if (!d->deserialize ((const char*)item->val, used))
        {
            PyErr_SetString (HeliumDbException, "failed to deserialize cdr");
            return NULL;
        }

        if (!d->getArray (0, &entries))
            continue;

        for (cdrArray::const_iterator it = entries->begin (); it != entries->end (); ++it)
        {
            if (cdr_utils_query (const_cast<cdr*>(&(*it)), query))
            {
                Py_BEGIN_ALLOW_THREADS
                rc = he_delete (self->mDatastore, item);
                Py_END_ALLOW_THREADS

                if (rc != 0)
                {
                    PyErr_SetString (HeliumDbException, he_strerror (errno));
                    return NULL;
                }

                if (deleteOne)
                {
                    Py_INCREF (Py_None);
                    return Py_None;
                }
            }
        }
    }

    delete d;
    Py_INCREF (Py_None);
    return Py_None;
}

PyObject*
heliumdb_delete_one (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    return _filterDelete (self, args, kwargs, true);
}

PyObject*
heliumdb_delete (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    return _filterDelete (self, args, kwargs, false);
}

static PyMethodDef heliumtsPy_methods[] = {
    {"insert_one", (PyCFunction)heliumdb_insert_one, METH_VARARGS | METH_KEYWORDS,
     "insert a single dictionary object"},
    {"insert_many", (PyCFunction)heliumdb_insert_many, METH_VARARGS | METH_KEYWORDS,
     "insert a single dictionary object"},
    {"find", (PyCFunction)heliumdb_find, METH_VARARGS | METH_KEYWORDS,
     "find values using the given query"},
    {"find_one", (PyCFunction)heliumdb_find_one, METH_VARARGS | METH_KEYWORDS,
     "find the first value using the given query"},
    {"delete_one", (PyCFunction)heliumdb_delete_one, METH_VARARGS | METH_KEYWORDS,
     "find first value using the given query, and delete"},
    {"delete", (PyCFunction)heliumdb_delete, METH_VARARGS | METH_KEYWORDS,
     "find values using the given query, and delete"},
    { NULL, NULL, 0, NULL }
};

PyTypeObject heliumtsPyType = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT (&PyType_Type, 0)
#else
    PyObject_HEAD_INIT (NULL)
    0,                                          /*ob_size*/
#endif
    "helium.Ts",                        /*tp_name*/
    sizeof(heliumtsPy),                         /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)heliumtsPy_dealloc,             /*tp_dealloc*/
    0,                                          /*tp_print*/
    0,                                          /*tp_getattr*/
    0,                                          /*tp_setattr*/
    0,                                          /*tp_as_sync*/
    0,                                          /*tp_repr*/
    0,                                          /*tp_as_number*/
    0,                                          /*tp_as_sequence*/
    0,                                          /*tp_as_mapping*/
    0,                                          /*tp_hash */
    0,                                          /*tp_call*/
    0,                                          /*tp_str*/
    0,                                          /*tp_getattro*/
    0,                                          /*tp_setattro*/
    0,                                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,                         /*tp_flags*/
    "HeliumTs",                                 /*tp_doc */
    0,                                          /*tp_traverse */
    0,                                          /*tp_clear */
    0,                                          /*tp_richcompare */
    0,                                          /*tp_weaklistoffset */
    0,                                          /*tp_iter */
    0,                                          /*tp_iternext */
    heliumtsPy_methods,                         /*tp_methods */
    0,                                          /*tp_members */
    0,                                          /*tp_getset */
    &heliumdbPyType,                            /*tp_base */
    0,                                          /*tp_dict */
    0,                                          /*tp_descr_get */
    0,                                          /*tp_descr_set */
    0,                                          /*tp_dictoffset */
    (initproc)heliumtsPy_init,                  /*tp_init */
    0,                                          /*tp_alloc */
    0,                                          /*tp_new */
    0,                                          /*tp_free*/
    0,                                          /*tp_is_gc*/
    0,                                          /*tp_bases*/
    0,                                          /*tp_mro*/
    0,                                          /*tp_cache*/
    0,                                          /*tp_subclasses*/
    0,                                          /*tp_weaklist*/
    0,                                          /*tp_del*/
    0                                           /*tp_version_tag*/
};
