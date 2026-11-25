/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
