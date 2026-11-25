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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Title: the RsParse class, a simple ORM implements of parser.
 * <p> aaa
 * Description:
 *
 * @author justbk
 * @version [Tools 0.0.1, 2023/11/11]
 * @since 2023/11/11
 */
public interface RsParser<T> {
    default T parse(ResultSet rs) {
        Class<T> type = getGenericType();
        Field[] fields = type.getDeclaredFields();
        T obj = null;
        try {
            obj = type.newInstance();
            for (Field f : fields) {
                f.setAccessible(true);
                try {
                    f.set(obj, parseObj(f, rs));
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                }
            }
        } catch (Exception exp) {
            exp.printStackTrace();
        }
        return obj;
    }
    
    default Class<T> getGenericType() {
        Class<T> type = (Class<T>) ((ParameterizedType) getClass()
                .getGenericInterfaces()[0]).getActualTypeArguments()[0];
        return type;
    }
    
    default Object parseObj(Field field, ResultSet rs) throws SQLException {
        Class<?> clz = field.getType();
        String name = field.getName().toLowerCase(Locale.ENGLISH);
        return RsTypeFactory.getValue(clz, rs, name);
    }
}
