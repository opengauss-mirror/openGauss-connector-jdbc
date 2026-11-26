/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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

package org.postgresql.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Helper class to instantiate objects. Note: the class is <b>NOT</b> public API, so it is subject
 * to change.
 */
public class ObjectFactory {

    /**
     *
     *  Instantiates a class using the appropriate constructor. If a constructor with a single
     *  Propertiesparameter exists, it is used. Otherwise, if tryString is true a constructor with a
     *  single String argument is searched if it fails, or tryString is true a no argument constructor
     *  is tried.
     *
     * @param expectedClass expected to inherit from the parent class
     * @param classname classname name of the class to instantiate
     * @param info parameter to pass as Properties
     * @param tryString weather to look for a single String argument constructor
     * @param stringarg parameter to pass as String
     * @param <T> generic class
     * @return the instantiated class
     * @throws ClassNotFoundException if something goes wrong
     * @throws SecurityException if something goes wrong
     * @throws NoSuchMethodException if something goes wrong
     * @throws IllegalArgumentException if something goes wrong
     * @throws InstantiationException if something goes wrong
     * @throws IllegalAccessException if something goes wrong
     * @throws InvocationTargetException if something goes wrong
     */
    public static <T> T instantiate(Class<T> expectedClass, String classname, Properties info,
                                    boolean tryString, String stringarg) throws ClassNotFoundException,
            SecurityException,
            NoSuchMethodException,
            IllegalArgumentException, InstantiationException, IllegalAccessException,
            InvocationTargetException {
        Object[] args = {info};
        Constructor<? extends T> ctor = null;
        Class<? extends T> cls = Class.forName(classname).asSubclass(expectedClass);
        try {
            ctor = cls.getConstructor(Properties.class);
        } catch (NoSuchMethodException nsme) {
            if (tryString) {
                try {
                    ctor = cls.getConstructor(String.class);
                    args = new String[]{stringarg};
                } catch (NoSuchMethodException nsme2) {
                    tryString = false;
                }
            }
            if (!tryString) {
                ctor = cls.getConstructor((Class[]) null);
                args = null;
            }
        }
        return ctor.newInstance(args);
    }

}
