/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd.
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

/*
 * This package contains a slightly modified version of opencsv library
 * without unwanted functionality and dependencies, licensed under Apache 2.0.
 *
 * See https://search.maven.org/artifact/com.opencsv/opencsv/3.4/bundle
 * See http://opencsv.sf.net/
 */
package org.postgresql.util.csv;

/**
 * Enumeration used to tell the CSVParser what to consider null.
 * <p/>
 * EMPTY_SEPARATORS - two sequential separators are null.
 * EMPTY_QUOTES - two sequential quotes are null
 * BOTH - both are null
 * NEITHER - default.  Both are considered empty string.
 */
public enum CSVReaderNullFieldIndicator {
    EMPTY_SEPARATORS,
    EMPTY_QUOTES,
    BOTH,
    NEITHER;
}
