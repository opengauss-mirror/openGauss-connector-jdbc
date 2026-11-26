/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
