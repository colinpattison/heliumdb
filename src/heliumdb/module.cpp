#include "module.h"
#include "datetime.h"
#include <utility>
#include <sstream>
#include <iostream>
#include "he_utils.h"
#include "cdr_utils.h"

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

static void
setEnvVariable (PyObject* kwargs, const char* param, uint64_t val, uint64_t& o)
{
    PyObject* k = PyUnicode_FromString (param);
    if (PyDict_Contains (kwargs, k) == 1)
        o = val;
}

bool
millisFromMidnight (cdrDateTime& dt, int64_t& millis)
{
    int hour = dt.mHour;
    int mins = dt.mMinute;
    int secs = dt.mSecond;
    int nanos = dt.mNanosecond;

    millis = (hour * 3600  * 1000) + (mins * 60 * 1000) + (secs * 1000) + int (nanos / 1000000);
    return true;
}

PyObject*
heliumdb_contains (heliumdbPy* self, PyObject* k)
{
    he_item item;
    if (!self->mKeySerializer (k, item.key, item.key_len))
    {
        PyErr_SetString (HeliumDbException, "could not serialize key object");
        return NULL;
    }

    return PyBool_FromLong (he_exists (self->mDatastore, &item) == 0);
}

int
heliumdbPy_init (heliumdbPy* self, PyObject* args, PyObject* kwargs)
{
    char* url = NULL;
    char* datastore = NULL;
    char* key_type = NULL;
    char* val_type = NULL;
    int64_t index_field = 0;
    string argString ("|ssssLKKKKKKKKKK");
    
    uint64_t fanout;
    uint64_t gc_fanout;
    uint64_t write_cache;
    uint64_t read_cache;
    uint64_t auto_commit_period;
    uint64_t auto_clean_period;
    uint64_t clean_util_pct;
    uint64_t clean_dirty_pct;
    uint64_t retry_count;
    uint64_t retry_delay;
    uint64_t compress_threshold;

    char *kwlist[] = {(char*)"url",
                      (char*)"datastore",
                      (char*)"key_type",
                      (char*)"val_type",
                      (char*)"index_field",
                      (char*)"fanout",
                      (char*)"gc_fanout",
                      (char*)"write_cache",
                      (char*)"read_cache",
                      (char*)"auto_commit_period",
                      (char*)"auto_clean_period",
                      (char*)"clean_util_pct",
                      (char*)"clean_dirty_pct",
                      (char*)"retry_count",
                      (char*)"retry_delay",
                      (char*)"compress_threshold",
                      NULL};

    if (!PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     argString.c_str (),
                                     kwlist,
                                     &url,
                                     &datastore,
                                     &key_type,
                                     &val_type,
                                     &index_field,
                                     &fanout,
                                     &gc_fanout,
                                     &write_cache,
                                     &read_cache,
                                     &auto_commit_period,
                                     &auto_clean_period,
                                     &clean_util_pct,
                                     &clean_dirty_pct,
                                     &retry_count,
                                     &retry_delay,
                                     &compress_threshold))
        return -1;

    if (url == NULL)
    {
        PyErr_SetString (HeliumDbException, "missing required argument url");
        return -1;
    }

    if (datastore == NULL)
    {
        PyErr_SetString (HeliumDbException, "missing required argument datastore");
        return -1;
    }

    /* env setup */
    he_env env;
    memset (&env, 0, sizeof (env));
    setEnvVariable (kwargs, "fanout", fanout, env.fanout);
    setEnvVariable (kwargs, "gc_fanout", gc_fanout, env.gc_fanout);
    setEnvVariable (kwargs, "write_cache", write_cache, env.write_cache);
    setEnvVariable (kwargs, "read_cache", read_cache, env.read_cache);
    setEnvVariable (kwargs, "auto_commit_period", auto_commit_period, env.auto_commit_period);
    setEnvVariable (kwargs, "auto_clean_period", auto_clean_period, env.auto_clean_period);
    setEnvVariable (kwargs, "clean_util_pct", clean_util_pct, env.clean_util_pct);
    setEnvVariable (kwargs, "clean_dirty_pct", clean_dirty_pct, env.clean_dirty_pct);
    setEnvVariable (kwargs, "retry_count", retry_count, env.retry_count);
    setEnvVariable (kwargs, "retry_delay", retry_delay, env.retry_delay);
    setEnvVariable (kwargs, "compress_threshold", compress_threshold, env.compress_threshold);

    if (self->mDatastore == NULL)
    {
        int options = HE_O_VOLUME_CREATE | HE_O_CREATE;
        self->mDatastore = he_open (url, datastore, options, &env);
        if (!self->mDatastore)
        {
            PyErr_SetString (HeliumDbException, he_strerror (errno));
            return -1;
        }
    }

    self->mIndexField = index_field;

    if (key_type == NULL || strcmp (key_type, "O") == 0)
    {
        self->mKeySerializer = &serializeObject;
        self->mKeyDeserializer = &deserializeObject;
    }
    else if (strcmp (key_type, "b") == 0)
    {
        self->mKeySerializer = &serializeBytes;
        self->mKeyDeserializer = &deserializeBytes;
    }
    else if (strcmp (key_type, "i") == 0)
    {
        self->mKeySerializer = &serializeIntKey;
        self->mKeyDeserializer = &deserializeInt;
    }
    else if (strcmp (key_type, "s") == 0)
    {
        self->mKeySerializer = &serializeString;
        self->mKeyDeserializer = &deserializeString;
    }
    else if (strcmp (key_type, "f") == 0)
    {
        self->mKeySerializer = &serializeFloatKey;
        self->mKeyDeserializer = &deserializeFloat;
    }
    else
    {
        PyErr_SetString (HeliumDbException, "unsupported key_type");
        return -1;
    }

    if (val_type == NULL || strcmp (val_type, "O") == 0)
    {
        self->mValSerializer = &serializeObject;
        self->mValDeserializer = &deserializeObject;
    }
    else if (strcmp (val_type, "b") == 0)
    {
        self->mValSerializer = &serializeBytes;
        self->mValDeserializer = &deserializeBytes;
    }
    else if (strcmp (val_type, "i") == 0)
    {
        self->mValSerializer = &serializeIntVal;
        self->mValDeserializer = &deserializeInt;
    }
    else if (strcmp (val_type, "s") == 0)
    {
        self->mValSerializer = &serializeString;
        self->mValDeserializer = &deserializeString;
    }
    else if (strcmp (val_type, "f") == 0)
    {
        self->mValSerializer = &serializeFloatVal;
        self->mValDeserializer = &deserializeFloat;
    }
    else if (strcmp (val_type, "C") == 0)
    {
        self->mValDeserializer = &cdr_utils_deserializeCdr;
    }
    else
    {
        PyErr_SetString (HeliumDbException, "unsupported val_type");
        return -1;
    }

    return 0;
}

static void
heliumdbPy_dealloc (heliumdbPy* self)
{
    Py_BEGIN_ALLOW_THREADS
    he_close (self->mDatastore);
    Py_END_ALLOW_THREADS
    Py_TYPE (self)->tp_free((PyObject*)self);
}

static PyObject*
heliumdb_cleanup (heliumdbPy* self)
{
    int rc;
    Py_BEGIN_ALLOW_THREADS
    rc = he_remove (self->mDatastore);
    Py_END_ALLOW_THREADS
    if (rc)
    {
        PyErr_SetString (HeliumDbException, he_strerror (errno));
        return NULL;
    }

    Py_INCREF (Py_None);
    return Py_None;
}

static PyObject*
heliumdb_get (heliumdbPy *self, PyObject *args)
{
    PyObject *k = NULL;
    PyObject *failobj = NULL;

    if (!PyArg_UnpackTuple (args, "get", 1, 2, &k, &failobj))
        return NULL; 

    he_item item;
    if (!self->mKeySerializer (k, item.key, item.key_len))
        return NULL;

    int rc;
    Py_BEGIN_ALLOW_THREADS
    rc = he_exists (self->mDatastore, &item);
    Py_END_ALLOW_THREADS

    if (rc != 0)
    {
        if (failobj == NULL)
        {
            PyErr_SetString (HeliumDbException, "key not found");
            return NULL;
        }
        Py_INCREF (failobj);
        return failobj;
    }

    return heliumdb_subscript (self, k);
}

static PyObject*
heliumdb_del (heliumdbPy* self, PyObject *args)
{
    PyObject *k;
    PyObject *failobj = Py_None;

    char* buffer[8096] = {0};
    size_t rdSize = sizeof (buffer);
    void* buf = NULL;

    if (!PyArg_UnpackTuple (args, "del", 1, 2, &k, &failobj))
    {
        PyErr_SetString (HeliumDbException, "could not unpack tuple args");
        return NULL;
    }
 
    he_item item;
    if (!self->mKeySerializer (k, item.key, item.key_len))
    {
        PyErr_SetString (HeliumDbException, "could not serialize key object");
        return NULL;
    }
    item.val = (void*)buffer;

    int rc;
    for (;;)
    {
        Py_BEGIN_ALLOW_THREADS
        rc = he_delete_lookup (self->mDatastore, &item, 0, rdSize);
        Py_END_ALLOW_THREADS

        if (rc != 0)
        {
            PyErr_SetString (HeliumDbException, he_strerror (errno));
            Py_INCREF (failobj);
            return failobj;
        }
        if (item.val_len > rdSize)
        {
            rdSize = item.val_len;
            buf = malloc (rdSize);
            item.val = buf;
        }

        else
        {
            break;
        }
    }

    PyObject* obj = self->mKeyDeserializer (item.val, item.val_len);
    if (obj == NULL)
    {
        PyErr_SetString (HeliumDbException, "failed to deserialize key object");
        return NULL;
    }

    if (buf)
        free (buf);

    return obj;
}

int
heliumdb_ass_sub (heliumdbPy* self, PyObject* k, PyObject* v)
{
    char err[128];
    he_item item;
    int rc;

    if (!self->mKeySerializer (k, item.key, item.key_len))
    {
        PyErr_SetString (HeliumDbException, "could not serialize key object");
        return -1;
    }

    if (v == NULL)
    {
        // delete
        char buffer[2] = {0};
        item.val = (void*)buffer;
        item.val_len = sizeof (buffer);

        Py_BEGIN_ALLOW_THREADS
        rc = he_delete_lookup (self->mDatastore, &item, 0, 1024);
        Py_END_ALLOW_THREADS

        if (rc != 0)
        {
            snprintf (err, 128, "he_delete_lookup failed: %s", he_strerror (errno));
            PyErr_SetString (HeliumDbException, err);
            return -1;
        }
        return 0;
    }

    if (!self->mValSerializer (v, item.val, item.val_len))
    {
        PyErr_SetString (HeliumDbException, "could not serialize value object");
        return -1;
    }

    if (!he_utils_update (self->mDatastore, item))
        return -1;

    return 0;
}

bool
_retrieveItem (he_t& he, he_item& getItem)
{
    char*   buffer[8096] = {0};
    size_t  rdSize = sizeof (buffer);
    void*   buf = NULL;

    getItem.val = (void*)buffer;

    int rc;
    for (;;)
    {
		Py_BEGIN_ALLOW_THREADS
        rc = he_lookup (he, &getItem, 0, rdSize);
        Py_END_ALLOW_THREADS

        if (rc != 0)
        {
            PyErr_SetString (HeliumDbException, "he_lookup failed");
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


PyObject*
heliumdb_subscript (heliumdbPy* self, PyObject* k)
{
    he_item getItem;
    if (!self->mKeySerializer (k, getItem.key, getItem.key_len))
    {
        PyErr_SetString (HeliumDbException, "could not serialize key object");
        return NULL;
    }

    _retrieveItem (self->mDatastore, getItem);

    PyObject* obj = self->mValDeserializer (getItem.val, getItem.val_len);
    if (obj == NULL)
    {
        PyErr_SetString (HeliumDbException, "failed to deserialize value object");
        return NULL;
    }

    return obj;
}

PyObject*
heliumdb_keys (heliumdbPy* self)
{
    vector<PyObject*>   keys;
    he_iter_t           itr;
    const he_item*      item;
    PyObject*           k;

    if (!(itr = he_iter_open (self->mDatastore, NULL, 0, 0, 0)))
    {
        PyErr_SetString (HeliumDbException, "failed to open iterator");
        return NULL;
    }

    while ((item = he_iter_next (itr)))
    {
        k = self->mKeyDeserializer (item->key, item->key_len);
        if (k == NULL)
        {
            PyErr_SetString (HeliumDbException, "failed to deserialize key");
            return NULL;
        }
        keys.push_back (k);
    }

    he_iter_close (itr);

    PyObject* keyList = PyList_New (keys.size ());

    vector<PyObject*>::iterator kitr = keys.begin ();

    int j = 0;
    for (; kitr != keys.end (); ++kitr)
        PyList_SetItem (keyList, j++, *kitr);

    return keyList;
}

PyObject*
heliumdb_stats (heliumdbPy* self)
{
    PyObject* res = PyDict_New ();

    if (res == NULL)
        return NULL;

    struct he_stats stats;

    int rc;
    Py_BEGIN_ALLOW_THREADS
    rc = he_stats (self->mDatastore, &stats);
    Py_END_ALLOW_THREADS

    if (rc != 0)
        return NULL;

    PyObject* name = PyUnicode_FromString (stats.name);
    if (PyDict_SetItemString (res, "name", name) < 0) 
    {
        Py_DECREF (name);
        return NULL;
    }

    PyObject* valid_items = PyLong_FromUnsignedLongLong (stats.valid_items);
    if (PyDict_SetItemString (res, "valid_items", valid_items) < 0) 
    {
        Py_DECREF (valid_items);
        return NULL;
    }

    PyObject* deleted_items = PyLong_FromUnsignedLongLong (stats.deleted_items);
    if (PyDict_SetItemString (res, "deleted_items", deleted_items) < 0) 
    {
        Py_DECREF (deleted_items);
        return NULL;
    }

    PyObject* utilized = PyLong_FromUnsignedLongLong (stats.utilized);
    if (PyDict_SetItemString (res, "utilized", utilized) < 0) 
    {
        Py_DECREF (utilized);
        return NULL;
    }

    PyObject* capacity = PyLong_FromUnsignedLongLong (stats.capacity);
    if (PyDict_SetItemString (res, "capacity", capacity) < 0) 
    {
        Py_DECREF (capacity);
        return NULL;
    }

    PyObject* buffered_writes = PyLong_FromUnsignedLongLong (stats.buffered_writes);
    if (PyDict_SetItemString (res, "buffered_writes", buffered_writes) < 0) 
    {
        Py_DECREF (buffered_writes);
        return NULL;
    }

    PyObject* buffered_reads = PyLong_FromUnsignedLongLong (stats.buffered_reads);
    if (PyDict_SetItemString (res, "buffered_reads", buffered_reads) < 0) 
    {
        Py_DECREF (buffered_reads);
        return NULL;
    }

    PyObject* dirty_writes = PyLong_FromUnsignedLongLong (stats.dirty_writes);
    if (PyDict_SetItemString (res, "dirty_writes", dirty_writes) < 0) 
    {
        Py_DECREF (dirty_writes);
        return NULL;
    }

    PyObject* device_writes = PyLong_FromUnsignedLongLong (stats.device_writes);
    if (PyDict_SetItemString (res, "device_writes", device_writes) < 0) 
    {
        Py_DECREF (device_writes);
        return NULL;
    }

    PyObject* device_reads = PyLong_FromUnsignedLongLong (stats.device_reads);
    if (PyDict_SetItemString (res, "device_reads", device_reads) < 0) 
    {
        Py_DECREF (device_reads);
        return NULL;
    }

    PyObject* auto_commits = PyLong_FromUnsignedLongLong (stats.auto_commits);
    if (PyDict_SetItemString (res, "auto_commits", auto_commits) < 0) 
    {
        Py_DECREF (auto_commits);
        return NULL;
    }

    PyObject* auto_cleans = PyLong_FromUnsignedLongLong (stats.auto_cleans);
    if (PyDict_SetItemString (res, "auto_cleans", auto_cleans) < 0) 
    {
        Py_DECREF (auto_cleans);
        return NULL;
    }

    PyObject* clean_bytes = PyLong_FromUnsignedLongLong (stats.clean_bytes);
    if (PyDict_SetItemString (res, "clean_bytes", clean_bytes) < 0) 
    {
        Py_DECREF (clean_bytes);
        return NULL;
    }

    PyObject* cache_hits = PyLong_FromUnsignedLongLong (stats.cache_hits);
    if (PyDict_SetItemString (res, "cache_hits", cache_hits) < 0) 
    {
        Py_DECREF (cache_hits);
        return NULL;
    }

    PyObject* cache_misses = PyLong_FromUnsignedLongLong (stats.cache_misses);
    if (PyDict_SetItemString (res, "cache_misses", cache_misses) < 0) 
    {
        Py_DECREF (cache_misses);
        return NULL;
    }

    Py_INCREF (res);
    return res;
}

PyObject*
heliumdb_commit (heliumdbPy* self)
{
    // TODO implement transaction handling
    int rc;
    Py_BEGIN_ALLOW_THREADS
    rc = he_commit (self->mDatastore);
    Py_END_ALLOW_THREADS

    if (rc != 0)
    {
        char buffer[128];
        snprintf (buffer, 128, "commit failed: %s", he_strerror (errno));
        PyErr_SetString (HeliumDbException, buffer);
        return NULL;
    }

    Py_INCREF (Py_None);
    return Py_None;
}

Py_ssize_t
heliumdb_len (heliumdbPy* self)
{
    struct he_stats stats;
    int rc;
    Py_BEGIN_ALLOW_THREADS
    rc = he_stats (self->mDatastore, &stats);
    Py_END_ALLOW_THREADS

    if (rc != 0)
        return 0;

    return PyLong_AsSsize_t (PyLong_FromUnsignedLongLong (stats.valid_items));
}

PyObject*
heliumdb_sizeof (heliumdbPy* self)
{
    struct he_stats stats;
    int rc;

    Py_BEGIN_ALLOW_THREADS
    rc = he_stats (self->mDatastore, &stats);
    Py_END_ALLOW_THREADS

    if (rc != 0)
        return NULL;

    return PyLong_FromUnsignedLongLong (stats.valid_items);
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
    cdr*            d;

    for (int i = 0; i < PyList_Size (data); i++)
    {
        PyObject* pycdr = PyList_GetItem (data, i);

        // if (!cdr_check (pycdr))
        //     return NULL;

        if (!cdr_utils_toCdr (pycdr, d))
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

            if (!_heliumUpdateCdr (self->mDatastore, item, entries))
                return NULL;

            entries.clear ();
            lastKey = key;
        }

        entries.appendArray (0, *d);
    }

    item.key = &key;
    item.key_len = sizeof (key);

    if (!_heliumUpdateCdr (self->mDatastore, item, entries))
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
    if (!cdr_utils_toCdr (data, d))
    {
        PyErr_SetString (HeliumDbException, "failed to get cdr object");
        return NULL;
    }

    cdrDateTime t;
    if (!d->getDateTime (self->mIndexField, t))
    {
        PyErr_SetString (HeliumDbException, "failed to retrieve index field");
        return NULL;
    }

    int64_t millis = 0;
    millisFromMidnight (t, millis);
    int64_t key = (millis / 10) * 10;

    he_item item;
    item.key = &key;
    item.key_len = sizeof (key);

    cdr entries;
    if (!he_utils_exists (self->mDatastore, item))
    {
        entries.appendArray (0, *d);

        if (!_heliumUpdateCdr (self->mDatastore, item, entries))
        {
            PyErr_SetString (HeliumDbException, "update failed");
            return NULL;
        }
    }
    else
    {
        if (!_retrieveItem (self->mDatastore, item))
        {
            PyErr_SetString (HeliumDbException, "retrieve failed");
            return NULL;
        }

        entries.deserialize ((const char*)item.val, item.val_len);
        entries.appendArray (0, *d);

        if (!_heliumUpdateCdr (self->mDatastore, item, entries))
        {
            PyErr_SetString (HeliumDbException, "update failed");
            return NULL;
        }
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

    // cdr check
    // if (!PyDict_Check (qdict))
    //     return NULL;
    
    cdr* query;
    if (!cdr_utils_toCdr (qdict, query))
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
                    return cdr_utils_toPythonObj (new cdr(*it));
                else
                {
                    results.push_back(make_pair (*(int64_t*)item->key,
                                                 cdr_utils_toPythonObj (new cdr (*it))));
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

    // if (!PyDict_Check (qdict))
    //     return -1;

    cdr* query;
    if (!cdr_utils_toCdr (qdict, query))
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

/* TODO update length and subscript method to be useful*/
static PyMappingMethods heliumdb_as_mapping = {
    (lenfunc)heliumdb_len,                  /*mp_length*/
    (binaryfunc)heliumdb_subscript,         /*mp_subscript*/
    (objobjargproc)heliumdb_ass_sub,        /*mp_ass_subscript*/
};

static PyMethodDef heliumdbPy_methods[] = {
    {"contains",  (PyCFunction)heliumdb_contains, METH_O | METH_COEXIST,
     "True if H has a key k, else False"},
    {"__contains__", (PyCFunction)heliumdb_contains, METH_O | METH_COEXIST, "True if H has a key K, else False"},
    {"__sizeof__", (PyCFunction)heliumdb_sizeof, METH_NOARGS, "returns number valid entries"},
    {"commit", (PyCFunction)heliumdb_commit, METH_NOARGS, "commits a transaction to datastore"},
    {"get",  (PyCFunction)heliumdb_get, METH_VARARGS, "get value by key"},
    {"cleanup",  (PyCFunction)heliumdb_cleanup, METH_NOARGS, "delete all entries in data store"},
    {"pop",  (PyCFunction)heliumdb_del, METH_VARARGS, "delete dict entry by key"},
    {"stats",  (PyCFunction)heliumdb_stats, METH_NOARGS, "retrieve datastore statistics"},
    {"keys",  (PyCFunction)heliumdb_keys, METH_NOARGS, "return list of all keys"},
    {"values",  (PyCFunction)heliumdb_itervalues, METH_NOARGS, "return list of all values"},
    {"items", (PyCFunction)heliumdb_iteritems,    METH_NOARGS, "iterates items"},
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
    // placeholders
    // __eq__
    // __ne__
    
    { NULL, NULL, 0, NULL }
};

static PyMethodDef heliumdb_methods[] = {
    { NULL, NULL, 0, NULL }
};

PyTypeObject heliumdbPyType = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT (&PyType_Type, 0)
#else
    PyObject_HEAD_INIT (NULL)
    0,                                          /*ob_size*/
#endif
    "heliumdb.Heliumdb",                        /*tp_name*/
    sizeof(heliumdbPy),                         /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)heliumdbPy_dealloc,             /*tp_dealloc*/
    0,                                          /*tp_print*/
    0,                                          /*tp_getattr*/
    0,                                          /*tp_setattr*/
    0,                                          /*tp_as_sync*/
    0,                                          /*tp_repr*/
    0,                                          /*tp_as_number*/
    0,                                          /*tp_as_sequence*/
    &heliumdb_as_mapping,                       /*tp_as_mapping*/
    0,                                          /*tp_hash */
    0,                                          /*tp_call*/
    0,                                          /*tp_str*/
    0,                                          /*tp_getattro*/
    0,                                          /*tp_setattro*/
    0,                                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,                         /*tp_flags*/
    "HeliumDb wrapper",                         /*tp_doc */
    0,                                          /*tp_traverse */
    0,                                          /*tp_clear */
    0,                                          /*tp_richcompare */
    0,                                          /*tp_weaklistoffset */
    (getiterfunc)heliumdb_iter,                   /*tp_iter */
    0,                                          /*tp_iternext */
    heliumdbPy_methods,                         /*tp_methods */
    0,                                          /*tp_members */
    0,                                          /*tp_getset */
    0,                                          /* tp_base */
    0,                                          /*tp_dict */
    0,                                          /*tp_descr_get */
    0,                                          /*tp_descr_set */
    0,                                          /*tp_dictoffset */
    (initproc)heliumdbPy_init,                  /*tp_init */
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

#if PY_MAJOR_VERSION >= 3
static int heliumdb_traverse (PyObject *m, visitproc visit, void *arg) 
{
    Py_VISIT(GETSTATE(m)->error);
    return 0;
}

static int heliumdb_clear (PyObject *m) 
{
    Py_CLEAR(GETSTATE(m)->error);
    return 0;
}


static struct PyModuleDef moduledef = 
{
        PyModuleDef_HEAD_INIT,
        "heliumdb",
        NULL,
        sizeof(struct module_state),
        heliumdb_methods,
        NULL,
        heliumdb_traverse,
        heliumdb_clear,
        NULL
};

#define INITERROR return NULL

PyMODINIT_FUNC
PyInit_heliumdb (void)
#else
#define INITERROR return

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
initheliumdb (void)
#endif
{
    PyObject* m = NULL;

    PyEval_InitThreads ();

    heliumdbPyType.tp_new = PyType_GenericNew;
    if(PyType_Ready (&heliumdbPyType) < 0)
#if PY_MAJOR_VERSION >= 3
        return NULL;
#else
        return;
#endif

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create (&moduledef);
#else
    m = Py_InitModule ("heliumdb", heliumdb_methods);
#endif

    if (m == NULL)
#if PY_MAJOR_VERSION >= 3
        return NULL;
#else
        return;
#endif

    Py_INCREF (&heliumdbPyType);
    PyModule_AddObject (m, "Heliumdb", (PyObject*)&heliumdbPyType);

    HeliumDbException = PyErr_NewException ("heliumdb.HeliumdbException", NULL, NULL);
    Py_INCREF (HeliumDbException);
    PyModule_AddObject (m, "HeliumdbException", HeliumDbException);

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
