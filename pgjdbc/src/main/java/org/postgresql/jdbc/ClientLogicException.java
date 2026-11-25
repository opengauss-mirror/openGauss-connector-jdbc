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

package org.postgresql.jdbc;

import java.sql.SQLException;

public class ClientLogicException extends SQLException {
	/**
     * The serial version UID for jvm serialization
     */
	private static final long serialVersionUID = 1148581951725038527L;
	
	private int errorCode;
	private String errorText;

	/* 
	 * Need to specify if the error is parsing error 
	 * we should not block bad queries to be sent to the server
	 * PgConnection.isValid is based on error that is not parsed correctly
	 */
	private boolean parsingError = false;
	
	/**
	 * @param errorCode - JNI lib error code
	 * @param errorMessage - JNI lib error message
	 */
	public ClientLogicException(int errorCode, String errorText) {
		super(errorText);
		this.errorCode = errorCode;
		this.errorText = errorText;
	}

    /**
	 * sets the parser error flag
	 * @param errorCode - JNI lib error code
	 * @param errorMessage - JNI lib error message
	 * @param parsingError - if the current issue is parsing issue
	 */
	public ClientLogicException(int errorCode, String errorText, boolean parsingError) {
		super(errorText);
		this.errorCode = errorCode;
		this.errorText = errorText;
		this.parsingError = parsingError;
	}

	/**
	 * @return error code
	 */
	public int getErrorCode() {
		return errorCode;
	}

	/**
	 * @return error mesage
	 */
	public String getErrorText() {
		return errorText;
	}

    /**
	 * @return check whether parsing error 
	 */
	public boolean isParsingError() {
		return parsingError;
	}

}
