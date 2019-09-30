#pragma once

#include "Python.h"
#include "he.h"
#include "module.h"

using namespace std;

bool he_utils_update (he_t& he, he_item& item);

bool he_utils_exists (he_t& he, he_item& item);

bool he_utils_retrieve (he_t& he, he_item& getItem);
