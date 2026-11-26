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
