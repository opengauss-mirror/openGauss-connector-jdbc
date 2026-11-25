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
package org.postgresql.util;

import org.postgresql.jdbc.PgArray;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Title: the RsTypeConvert class, convert database type to java type.
 * <p>
 * Description:
 *
 * @author justbk
 * @version [Tools 0.0.1, 2023/11/11]
 * @since 2023/11/11
 */
public class RsTypeFactory {
    @FunctionalInterface
    public interface RsTypeConvert {
        Object apply(ResultSet rs, String name) throws SQLException;
    }

    Map<Class, RsTypeConvert> converts = new HashMap<>();
    
    private static RsTypeConvert defaultConverts = (rs, name) -> rs.getObject(name);
    private static RsTypeConvert intConvert = (rs, name) -> rs.getInt(name);
    private static RsTypeConvert longConvert = (rs, name) -> rs.getLong(name);
    private static RsTypeConvert booleanConvert = (rs, name) -> rs.getBoolean(name);
    private static RsTypeConvert blobConvert = (rs, name) -> rs.getBlob(name);
    private static RsTypeConvert strConvert = (rs, name) -> rs.getString(name);
    private static RsTypeConvert pgArrayConvert = (rs, name) -> rs.getArray(name);
    private static RsTypeConvert doubleConvert = (rs, name) -> rs.getDouble(name);
    private static RsTypeConvert floatConvert = (rs, name) -> rs.getFloat(name);
    
    // must keep this instance under all convert!!!
    private static RsTypeFactory instance = new RsTypeFactory();
    
    private RsTypeFactory() {
        converts.put(int.class, intConvert);
        converts.put(Integer.class, intConvert);
        converts.put(long.class, longConvert);
        converts.put(Long.class, longConvert);
        converts.put(float.class, floatConvert);
        converts.put(Float.class, floatConvert);
        converts.put(double.class, doubleConvert);
        converts.put(Double.class, doubleConvert);
        converts.put(boolean.class, booleanConvert);
        converts.put(Boolean.class, booleanConvert);
        converts.put(Blob.class, blobConvert);
        converts.put(String.class, strConvert);
        converts.put(PgArray.class, pgArrayConvert);
    }
    
    public static final RsTypeFactory getInstance() {
        return instance;
    }

    public RsTypeConvert getConvert(Class clz) {
        return converts.getOrDefault(clz, defaultConverts);
    }

    public static Object getValue(Class clz, ResultSet rs, String name) throws SQLException {
        return getInstance().getConvert(clz).apply(rs, name);
    }
}
