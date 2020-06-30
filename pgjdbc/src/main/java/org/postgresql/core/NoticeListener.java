/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2011, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/
/*NOTICE RAISE*/
package org.postgresql.core;

import org.postgresql.PGStatement;
import java.sql.*;
import java.util.Vector;

public interface NoticeListener
{
	public void noticeReceived(SQLWarning warn) ;
}
