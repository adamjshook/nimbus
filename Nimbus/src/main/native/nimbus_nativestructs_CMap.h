/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class nimbus_nativestructs_CMap */

#ifndef _Included_nimbus_nativestructs_CMap
#define _Included_nimbus_nativestructs_CMap
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_clear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_nimbus_nativestructs_CMap_c_1clear
  (JNIEnv *, jobject);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_containsKey
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_nativestructs_CMap_c_1containsKey
  (JNIEnv *, jobject, jobject);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_containsValue
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_nativestructs_CMap_c_1containsValue
  (JNIEnv *, jobject, jobject);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_get
 * Signature: (Ljava/lang/Object;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_nimbus_nativestructs_CMap_c_1get
  (JNIEnv *, jobject, jobject);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_isEmpty
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_nimbus_nativestructs_CMap_c_1isEmpty
  (JNIEnv *, jobject);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_put
 * Signature: (Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_nimbus_nativestructs_CMap_c_1put
  (JNIEnv *, jobject, jstring, jstring);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_remove
 * Signature: (Ljava/lang/Object;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_nimbus_nativestructs_CMap_c_1remove
  (JNIEnv *, jobject, jobject);

/*
 * Class:     nimbus_nativestructs_CMap
 * Method:    c_size
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_nimbus_nativestructs_CMap_c_1size
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
