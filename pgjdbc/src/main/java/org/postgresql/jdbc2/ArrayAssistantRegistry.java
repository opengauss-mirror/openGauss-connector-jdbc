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

package org.postgresql.jdbc2;

import java.util.HashMap;
import java.util.Map;

/**
 * Array assistants register here.
 *
 * @author Minglei Tu
 */
public class ArrayAssistantRegistry {
  private static Map<Integer, ArrayAssistant> arrayAssistantMap =
      new HashMap<Integer, ArrayAssistant>();

  public static ArrayAssistant getAssistant(int oid) {
    return arrayAssistantMap.get(oid);
  }

  ////
  public static void register(int oid, ArrayAssistant assistant) {
    arrayAssistantMap.put(oid, assistant);
  }
}
