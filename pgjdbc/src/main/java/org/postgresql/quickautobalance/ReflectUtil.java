/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.quickautobalance;

import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Reflect util
 */
public class ReflectUtil {
    private static Log LOGGER = Logger.getLogger(ReflectUtil.class.getName());

    /**
     *
     * @param classz classz
     * @param object object
     * @param methodName methodName
     */
    public static void invoke(Class classz, Object object, String methodName) {
        try {
            Method method = classz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            method.invoke(object);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            LOGGER.error(GT.tr("call reflect method {}.{} error.", classz, methodName));
        }
    }

    /**
     * Get the private property of an object.
     *
     * @param classz class of object
     * @param object object
     * @param t class of the private property
     * @param fieldName of the private property
     * @param <T> class of the private property
     * @return the private property
     */
    public static <T> T getField(Class classz, Object object, Class<T> t, String fieldName) {
        try {
            Field field = classz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.error("get reflect field " + classz + "." + fieldName + " error.");
        }
        return null;
    }

    /**
     * Set the private property of an object.
     *
     * @param classz class of object
     * @param object object
     * @param fieldName of the private property
     * @param value value of the private property
     */
    public static void setField(Class classz, Object object, String fieldName, Object value) {
        try {
            Field field = classz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.error("set reflect field " + classz + "." + fieldName + " error.");
        }
    }
}
