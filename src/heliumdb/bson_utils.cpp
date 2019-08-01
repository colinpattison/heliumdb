#include "bson_utils.h"
#include "utils.h"

bool
bson_query (bson_t*  doc, bson_t* query)
{
    bson_iter_t iter;
    bson_iter_t qiter;

    if (!bson_iter_init (&qiter, query))
        return false;

    const bson_value_t* docval;
    const bson_value_t* qval;

    while (bson_iter_next (&qiter))
    {
        if (!bson_iter_init_find (&iter, doc, bson_iter_key (&qiter)))
            return false;

        qval = bson_iter_value (&qiter);
        docval = bson_iter_value (&iter);

        if (docval->value_type == qval->value_type)
        {
            switch (docval->value_type)
            {
                case BSON_TYPE_UTF8:
                    if (strcmp (docval->value.v_utf8.str,
                                qval->value.v_utf8.str) == 0)
                        continue;
                    else
                        return false;
                case BSON_TYPE_SYMBOL:
                    if (strcmp (docval->value.v_symbol.symbol,
                                qval->value.v_symbol.symbol) == 0)
                        continue;
                    else
                        return false;
                case BSON_TYPE_INT32:
                    if (docval->value.v_int32 == qval->value.v_int32)
                        continue;
                    else
                        return false;
                case BSON_TYPE_INT64:
                    if (docval->value.v_int64 == qval->value.v_int64)
                        continue;
                    else
                        return false;
                case BSON_TYPE_BOOL:
                    if (docval->value.v_bool == qval->value.v_bool)
                        continue;
                    else
                        return false;
                case BSON_TYPE_DOUBLE:
                    if (fabs (docval->value.v_double - qval->value.v_double)
                            < EPSILON)
                        continue;
                    else
                        return false;
                default:
                    return false;
            }
        }
        else
            return false;
    }

    return true;
}

// bool
// bson_update (bson_t*  doc, bson_t* filter)
// {
//     bson_iter_t itr;
//     bson_iter_t fItr;
//
//     if (!bson_iter_init (&fItr, filter))
//         return false;
//
//     const bson_value_t* docval;
//     const bson_value_t* qval;
//
//     while (bson_iter_next (&qiter))
//     {
//         if (!bson_iter_init_find (&iter, doc, bson_iter_key (&qiter)))
//             return false;
//
//         qval = bson_iter_value (&qiter);
//         docval = bson_iter_value (&iter);
//
//         if (docval->value_type == qval->value_type)
//         {
//             switch (docval->value_type)
//             {
//                 case BSON_TYPE_UTF8:
//                     if (strcmp (docval->value.v_utf8.str,
//                                 qval->value.v_utf8.str) == 0)
//                         continue;
//                     else
//                         return false;
//                 case BSON_TYPE_SYMBOL:
//                     if (strcmp (docval->value.v_symbol.symbol,
//                                 qval->value.v_symbol.symbol) == 0)
//                         continue;
//                     else
//                         return false;
//                 case BSON_TYPE_INT32:
//                     if (docval->value.v_int32 == qval->value.v_int32)
//                         continue;
//                     else
//                         return false;
//                 case BSON_TYPE_INT64:
//                     if (docval->value.v_int64 == qval->value.v_int64)
//                         continue;
//                     else
//                         return false;
//                 case BSON_TYPE_BOOL:
//                     if (docval->value.v_bool == qval->value.v_bool)
//                         continue;
//                     else
//                         return false;
//                 case BSON_TYPE_DOUBLE:
//                     if (fabs (docval->value.v_double - qval->value.v_double)
//                             < EPSILON)
//                         continue;
//                     else
//                         return false;
//                 default:
//                     return false;
//             }
//         }
//         else
//             return false;
//     }
//
//     return true;
// }


bson_t*
dictToBson (PyObject* d)
{
    if (!PyDict_Check (d))
        return NULL;

    PyObject* bson = bsonEncodeObject (d);

    const char* bytes;
    size_t len;

#if PY_MAJOR_VERSION >= 3
    bytes = PyBytes_AsString (bson);
    len = PyBytes_Size (bson);
#else
    bytes = PyString_AsString (bson);
    len = PyString_Size (bson);
#endif
    if (bytes == NULL)
    {
        printf ("failed to serialize bson object");
        return false;
    }

    Py_DECREF (bson);
    return bson_new_from_data ((uint8_t*)bytes, len);
}
