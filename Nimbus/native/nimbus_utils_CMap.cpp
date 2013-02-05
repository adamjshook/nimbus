#include <jni.h>
#include <stdio.h>
#include "JNIUtils.h"
#include "nimbus_utils_CMap.h"
#include <map>
#include <cstring>
#include <functional>
#include <sstream>
#include <iostream>

struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};

std::map<const char*, const char*, ltstr> map;
typedef std::map<const char*, const char*, ltstr>::iterator mapiter;
typedef std::map<const char*, const char*, ltstr>::const_iterator mapconstiter;

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_clear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_nimbus_utils_CMap_c_1clear(JNIEnv *env, jobject obj)
{
    mapiter iter = map.begin();
	mapiter end = map.end();
	const char* value = 0;
	while (iter != end)
	{	
		delete [] iter->first;
        delete [] iter->second;

		++iter;
	}

    map.clear();
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_containsKey
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CMap_c_1containsKey(JNIEnv *env, jobject obj, jobject key)
{
    return map.find(JNIUtils::strFromJString(env, (jstring)key)) != map.end();
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_containsValue
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CMap_c_1containsValue(JNIEnv *env, jobject obj, jobject value)
{
    mapconstiter iter = map.begin();
    mapconstiter end = map.end();

    const char* cValue = JNIUtils::strFromJString(env, (jstring)value);
    bool retval = false;

    while (iter != end)
    {
        if (strcmp(iter->second, cValue) == 0)
        {
            retval = true;
            break;
        }

        ++iter;
    }

    return retval;
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_get
 * Signature: (Ljava/lang/Object;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_nimbus_utils_CMap_c_1get(JNIEnv *env, jobject obj, jobject key)
{
    const char* cKey = JNIUtils::strFromJString(env, (jstring)key);

    mapconstiter iter = map.find(cKey);
    if (iter != map.end())
    {
        return JNIUtils::strToJString(env, iter->second);
    } 
    else
    {
        return 0;
    }
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_isEmpty
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CMap_c_1isEmpty(JNIEnv *env, jobject obj)
{
    return map.size() == 0;
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_put
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */

JNIEXPORT jstring JNICALL Java_nimbus_utils_CMap_c_1put(JNIEnv *env, jobject obj, jstring key, jstring value)
{
    const char* cKey = JNIUtils::strFromJString(env, key);
    const char* cValue = JNIUtils::strFromJString(env, value);

    mapiter iter = map.find(cKey);
    if (iter == map.end())
    {
        map.insert(std::pair<const char*, const char*>(cKey, cValue));
        return 0;
    }
    else
    {
        jstring retval = JNIUtils::strToJString(env, iter->second);

        delete [] iter->first;
        delete [] iter->second;

        map.erase(iter);

        map.insert(std::pair<const char*, const char*>(cKey, cValue));
        return retval;
    }
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_remove
 * Signature: (Ljava/lang/Object;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_nimbus_utils_CMap_c_1remove(JNIEnv *env, jobject obj, jobject key)
{
    const char* cKey = JNIUtils::strFromJString(env, (jstring)key);
    mapiter iter = map.find(cKey);
    if (iter != map.end())
    {
        jstring retval = JNIUtils::strToJString(env, iter->second);

		delete [] iter->first;
        delete [] iter->second;

        map.erase(iter);
        return retval;
    }
    else
    {
        return 0;
    }
}

/*
 * Class:     nimbus_utils_CMap
 * Method:    c_size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_nimbus_utils_CMap_c_1size(JNIEnv *env, jobject obj)
{
    return map.size();
}