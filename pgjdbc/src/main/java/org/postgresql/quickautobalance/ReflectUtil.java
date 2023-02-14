/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.quickautobalance;

import org.postgresql.log.Log;
import org.postgresql.log.Logger;

import java.lang.reflect.Field;

/**
 * Reflect util
 */
public class ReflectUtil {
    private static Log LOGGER = Logger.getLogger(ReflectUtil.class.getName());

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
