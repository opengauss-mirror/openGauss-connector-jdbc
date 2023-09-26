/*
 * Copyright (c) 2023, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
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
