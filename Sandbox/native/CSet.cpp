/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
#include <stdio.h>
#include "CSet.h"
#include <set>
#include <string>
#include <sstream>
#include <iostream>

std::set<std::string> set;


std::string strFromJString(JNIEnv* env, jstring element)
{
	const char* chars = env->GetStringUTFChars(element, NULL);
	std::string str((const char*)(chars));
	env->ReleaseStringUTFChars(element, chars);	
	return str;
}

/*
 * Class:     CSet
 * Method:    c_add
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_CSet_c_1add(JNIEnv *env, jobject obj, jstring element)
{
	return set.insert(strFromJString(env, element)).second;
}

/*
 * Class:     CSet
 * Method:    c_clear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_CSet_c_1clear(JNIEnv *env, jobject obj)
{
	set.clear();
}

/*
 * Class:     CSet
 * Method:    c_contains
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_CSet_c_1contains(JNIEnv *env, jobject obj, jstring element)
{
	return set.find(strFromJString(env, element)) != set.end();
}

/*
 * Class:     CSet
 * Method:    c_isEmpty
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_CSet_c_1isEmpty(JNIEnv *, jobject)
{
	return set.empty();
}

/*
 * Class:     CSet
 * Method:    c_remove
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_CSet_c_1remove(JNIEnv *env, jobject obj, jstring element)
{
	return set.erase(strFromJString(env, element));
}

/*
 * Class:     CSet
 * Method:    c_size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_CSet_c_1size(JNIEnv *, jobject)
{
	return set.size();
}

