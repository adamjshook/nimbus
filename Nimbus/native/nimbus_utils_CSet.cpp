#include <jni.h>
#include <stdio.h>
#include "JNIUtils.h"
#include "nimbus_utils_CSet.h"
#include <set>
#include <cstring>
#include <sstream>
#include <iostream>

struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};

std::set<const char*, ltstr> set;
typedef std::set<const char*, ltstr>::const_iterator setconstiter;
typedef std::set<const char*, ltstr>::iterator setiter;

/*
 * Class:     CSet
 * Method:    c_add
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CSet_c_1add(JNIEnv *env, jobject obj, jstring element)
{
	return set.insert(JNIUtils::strFromJString(env, element)).second;
}

/*
 * Class:     CSet
 * Method:    c_clear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_nimbus_utils_CSet_c_1clear(JNIEnv *env, jobject obj)
{
	setiter iter = set.begin();
	setiter end = set.end();
	const char* value = 0;
	while (iter != end)
	{
		value = *iter;
		delete [] value;
		value = 0;
		++iter;
	}

	set.clear();
}

/*
 * Class:     CSet
 * Method:    c_contains
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CSet_c_1contains(JNIEnv *env, jobject obj, jstring element)
{
	return set.find(JNIUtils::strFromJString(env, element)) != set.end();
}

/*
 * Class:     CSet
 * Method:    c_isEmpty
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CSet_c_1isEmpty(JNIEnv *, jobject)
{
	return set.empty();
}

/*
 * Class:     CSet
 * Method:    c_remove
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_utils_CSet_c_1remove(JNIEnv *env, jobject obj, jstring element)
{
	setiter iter = set.find(JNIUtils::strFromJString(env, element));
	if (iter != set.end())
	{
		const char* value = *iter;

		set.erase(iter);

		delete [] value;
		value = 0;

		return true;
	}
	else
	{
		return false;
	}
}

/*
 * Class:     CSet
 * Method:    c_size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_nimbus_utils_CSet_c_1size(JNIEnv *, jobject)
{
	return set.size();
}
