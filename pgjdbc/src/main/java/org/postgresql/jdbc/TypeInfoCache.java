/*
 * Copyright (c) 2005, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Oid;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.TypeInfo;
import org.postgresql.util.GT;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TypeInfoCache implements TypeInfo {

  // pgname (String) -> java.sql.Types (Integer)
  private Map<String, Integer> _pgNameToSQLType;

  // pgname (String) -> java class name (String)
  // ie "text" -> "java.lang.String"
  private Map<String, String> _pgNameToJavaClass;

  // oid (Integer) -> pgname (String)
  private Map<Integer, String> _oidToPgName;
  // pgname (String) -> oid (Integer)
  private Map<String, Integer> _pgNameToOid;

  // pgname (String) -> extension pgobject (Class)
  private Map<String, Class<? extends PGobject>> _pgNameToPgObject;

  // type array oid -> base type's oid
  private Map<Integer, Integer> _pgArrayToPgType;

  // array type oid -> base type array element delimiter
  private Map<Integer, Character> _arrayOidToDelimiter;

  private BaseConnection _conn;
  private final int _unknownLength;
  private PreparedStatement _getOidStatementSimple;
  private PreparedStatement getOidStatementComplexNonArray;
  private PreparedStatement getOidStatementComplexArray;
  private PreparedStatement _getNameStatement;
  private PreparedStatement _getTypeNameByOIDStatement;
  private PreparedStatement _getArrayElementOidStatement;
  private PreparedStatement _getArrayDelimiterStatement;
  private PreparedStatement _getTypeInfoStatement;

  // basic pg types info:
  // 0 - type name
  // 1 - type oid
  // 2 - sql type
  // 3 - java class
  // 4 - array type oid
  private static final Object[][] types = {
	  {"int1", Oid.INT1, Types.TINYINT, "java.lang.Integer", Oid.INT1_ARRAY},	
      {"int2", Oid.INT2, Types.SMALLINT, "java.lang.Integer", Oid.INT2_ARRAY},
      {"int4", Oid.INT4, Types.INTEGER, "java.lang.Integer", Oid.INT4_ARRAY},
      {"oid", Oid.OID, Types.BIGINT, "java.lang.Long", Oid.OID_ARRAY},
      {"int8", Oid.INT8, Types.BIGINT, "java.lang.Long", Oid.INT8_ARRAY},
      {"money", Oid.MONEY, Types.DOUBLE, "java.lang.Double", Oid.MONEY_ARRAY},
      {"numeric", Oid.NUMERIC, Types.NUMERIC, "java.math.BigDecimal", Oid.NUMERIC_ARRAY},
      {"float4", Oid.FLOAT4, Types.REAL, "java.lang.Float", Oid.FLOAT4_ARRAY},
      {"float8", Oid.FLOAT8, Types.DOUBLE, "java.lang.Double", Oid.FLOAT8_ARRAY},
      {"char", Oid.CHAR, Types.CHAR, "java.lang.String", Oid.CHAR_ARRAY},
      {"bpchar", Oid.BPCHAR, Types.CHAR, "java.lang.String", Oid.BPCHAR_ARRAY},
      {"varchar", Oid.VARCHAR, Types.VARCHAR, "java.lang.String", Oid.VARCHAR_ARRAY},
      {"text", Oid.TEXT, Types.VARCHAR, "java.lang.String", Oid.TEXT_ARRAY},
      {"name", Oid.NAME, Types.VARCHAR, "java.lang.String", Oid.NAME_ARRAY},
      {"bytea", Oid.BYTEA, Types.BINARY, "[B", Oid.BYTEA_ARRAY},
      {"bool", Oid.BOOL, Types.BIT, "java.lang.Boolean", Oid.BOOL_ARRAY},
      {"bit", Oid.BIT, Types.BIT, "java.lang.Boolean", Oid.BIT_ARRAY},
      {"date", Oid.DATE, Types.DATE, "java.sql.Date", Oid.DATE_ARRAY},
      {"time", Oid.TIME, Types.TIME, "java.sql.Time", Oid.TIME_ARRAY},
      {"timetz", Oid.TIMETZ, Types.TIME, "java.sql.Time", Oid.TIMETZ_ARRAY},
      {"timestamp", Oid.TIMESTAMP, Types.TIMESTAMP, "java.sql.Timestamp", Oid.TIMESTAMP_ARRAY},
      {"smalldatetime", Oid.SMALLDATETIME, Types.TIMESTAMP, "java.lang.Timestamp", Oid.SMALLDATETIME_ARRAY},
      {"timestamptz", Oid.TIMESTAMPTZ, Types.TIMESTAMP, "java.sql.Timestamp",
          Oid.TIMESTAMPTZ_ARRAY},
      {"json", Oid.JSON, Types.OTHER, "org.postgresql.util.PGobject", Oid.JSON_ARRAY},
      {"point", Oid.POINT, Types.OTHER, "org.postgresql.geometric.PGpoint", Oid.POINT_ARRAY},
      {"blob", Oid.BLOB, Types.BLOB, "org.postgresql.util.PGobject", -1},
      {"clob", Oid.CLOB, Types.CLOB, "org.postgresql.util.PGobject", -1},
      {"nvarchar2", Oid.NVARCHAR2, Types.VARCHAR, "java.lang.String", Oid.NVARCHAR2_ARRAY},
      {"refcursor", Oid.REF_CURSOR, Types.REF_CURSOR, "java.sql.ResultSet", Oid.REF_CURSOR_ARRAY}
  };

  /**
   * PG maps several alias to real type names. When we do queries against pg_catalog, we must use
   * the real type, not an alias, so use this mapping.
   */
  private static final HashMap<String, String> typeAliases;

  static {
    typeAliases = new HashMap<String, String>();
    typeAliases.put("tinyint", "int1");
    typeAliases.put("smallint", "int2");
    typeAliases.put("integer", "int4");
    typeAliases.put("int", "int4");
    typeAliases.put("bigint", "int8");
    typeAliases.put("float", "float8");
    typeAliases.put("boolean", "bool");
    typeAliases.put("decimal", "numeric");
  }

  public TypeInfoCache(BaseConnection conn, int unknownLength) {
    _conn = conn;
    _unknownLength = unknownLength;
    _oidToPgName = new HashMap<Integer, String>();
    _pgNameToOid = new HashMap<String, Integer>();
    _pgNameToJavaClass = new HashMap<String, String>();
    _pgNameToPgObject = new HashMap<String, Class<? extends PGobject>>();
    _pgArrayToPgType = new HashMap<Integer, Integer>();
    _arrayOidToDelimiter = new HashMap<Integer, Character>();

    // needs to be synchronized because the iterator is returned
    // from getPGTypeNamesWithSQLTypes()
    _pgNameToSQLType = Collections.synchronizedMap(new HashMap<String, Integer>());

    for (Object[] type : types) {
      String pgTypeName = (String) type[0];
      Integer oid = (Integer) type[1];
      Integer sqlType = (Integer) type[2];
      String javaClass = (String) type[3];
      Integer arrayOid = (Integer) type[4];

      addCoreType(pgTypeName, oid, sqlType, javaClass, arrayOid);
    }

  }

  public synchronized void addCoreType(String pgTypeName, Integer oid, Integer sqlType,
      String javaClass, Integer arrayOid) {
    _pgNameToJavaClass.put(pgTypeName, javaClass);
    _pgNameToOid.put(pgTypeName, oid);
    _oidToPgName.put(oid, pgTypeName);
    _pgArrayToPgType.put(arrayOid, oid);
    _pgNameToSQLType.put(pgTypeName, sqlType);

    // Currently we hardcode all core types array delimiter
    // to a comma. In a stock install the only exception is
    // the box datatype and it's not a JDBC core type.
    //
    Character delim = ',';
    _arrayOidToDelimiter.put(oid, delim);

    String pgArrayTypeName = pgTypeName + "[]";
    _pgNameToJavaClass.put(pgArrayTypeName, "java.sql.Array");
    _pgNameToSQLType.put(pgArrayTypeName, Types.ARRAY);
    _pgNameToOid.put(pgArrayTypeName, arrayOid);
    pgArrayTypeName = "_" + pgTypeName;
    if (!_pgNameToJavaClass.containsKey(pgArrayTypeName)) {
      _pgNameToJavaClass.put(pgArrayTypeName, "java.sql.Array");
      _pgNameToSQLType.put(pgArrayTypeName, Types.ARRAY);
      _pgNameToOid.put(pgArrayTypeName, arrayOid);
      _oidToPgName.put(arrayOid, pgArrayTypeName);
    }
  }


  public synchronized void addDataType(String type, Class<? extends PGobject> klass)
      throws SQLException {
    _pgNameToPgObject.put(type, klass);
    _pgNameToJavaClass.put(type, klass.getName());
  }

  public Iterator<String> getPGTypeNamesWithSQLTypes() {
    return _pgNameToSQLType.keySet().iterator();
  }

  public int getSQLType(int oid) throws SQLException {
    return getSQLType(getPGType(oid));
  }

  public synchronized int getSQLType(String pgTypeName) throws SQLException {
    if (pgTypeName.endsWith("[]")) {
      return Types.ARRAY;
    }
    Integer i = _pgNameToSQLType.get(pgTypeName);
    if (i != null) {
      return i;
    }

    if (_getTypeInfoStatement == null) {
      // There's no great way of telling what's an array type.
      // People can name their own types starting with _.
      // Other types use typelem that aren't actually arrays, like box.
      //
    	  String sql = "SELECT typinput='array_in'::regproc, typtype FROM ";
          //                  -- go with older way of unnesting array to be compatible with 8.0
 
          sql += "pg_catalog.";
 
          sql += "pg_type WHERE typname = ?";

      _getTypeInfoStatement = _conn.prepareStatement(sql);
    }

    _getTypeInfoStatement.setString(1, pgTypeName);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) _getTypeInfoStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    ResultSet rs = _getTypeInfoStatement.getResultSet();

    Integer type = null;
    if (rs.next()) {
      boolean isArray = rs.getBoolean(1);
      String typtype = rs.getString(2);
      if (isArray) {
        type = Types.ARRAY;
      } else if ("c".equals(typtype)) {
        type = Types.STRUCT;
      } else if ("d".equals(typtype)) {
        type = Types.DISTINCT;
      } 
    }

    if (type == null) {
      type = Types.OTHER;
    }
    rs.close();

    _pgNameToSQLType.put(pgTypeName, type);
    return type;
  }

  private PreparedStatement getOidStatement(String pgTypeName) throws SQLException {
    boolean isArray = pgTypeName.endsWith("[]");
    boolean hasQuote = pgTypeName.contains("\"");
    int dotIndex = pgTypeName.indexOf('.');
  
    if (dotIndex == -1 && !hasQuote && !isArray) {
      if (_getNameStatement == null) {
        String sql;
        // see comments in @getSQLType()
        // -- go with older way of unnesting array to be compatible with 8.0
        sql = "SELECT pg_type.oid, typname "
                + "  FROM pg_catalog.pg_type "
                + "  LEFT "
                + "  JOIN (select ns.oid as nspoid, ns.nspname, r.r "
                + "          from pg_namespace as ns "
                + "          join ( select s.r, (current_schemas(false))[s.r] as nspname"
                + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
                + "         using ( nspname ) "
                + "       ) as sp "
                + "    ON sp.nspoid = typnamespace "
                + " WHERE typname = ? "
                + " ORDER BY sp.r, pg_type.oid DESC LIMIT 1;";
        _getNameStatement = _conn.prepareStatement(sql);
      }
      // coerce to lower case to handle upper case type names
      String lcName = pgTypeName.toLowerCase(Locale.ROOT);
      // default arrays are represented with _ as prefix ... this dont even work for public schema
      // fully
      _getNameStatement.setString(1, lcName);
      return _getNameStatement;
    }
    PreparedStatement oidStatementComplex;
    if (isArray) {
      if (getOidStatementComplexArray == null) {
        String sql;
        if ( _conn.haveMinimumServerVersion(ServerVersion.v8_3)) {
          sql = "SELECT t.typarray, arr.typname "
                  + "  FROM pg_catalog.pg_type t"
                  + "  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid"
                  + "  JOIN pg_catalog.pg_type arr ON arr.oid = t.typarray"
                  + " WHERE t.typname = ? AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))"
                  + " ORDER BY t.oid DESC LIMIT 1";
        } else {
          sql = "SELECT t.oid, t.typname "
                  + "  FROM pg_catalog.pg_type t"
                  + "  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid"
                  + " WHERE t.typelem = (SELECT oid FROM pg_catalog.pg_type WHERE typname = ?)"
                  + " AND substring(t.typname, 1, 1) = '_' AND t.typlen = -1"
                  + " AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))"
                  + " ORDER BY t.typelem DESC LIMIT 1";
        }
        getOidStatementComplexArray =  _conn.prepareStatement(sql);
      }
      oidStatementComplex = getOidStatementComplexArray;
    } else {
      if (getOidStatementComplexNonArray == null) {
        String sql = "SELECT t.oid, t.typname "
                + "  FROM pg_catalog.pg_type t"
                + "  JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid"
                + " WHERE t.typname = ? AND (n.nspname = ? OR ? AND n.nspname = ANY (current_schemas(true)))"
                + " ORDER BY t.oid DESC LIMIT 1";
        getOidStatementComplexNonArray =  _conn.prepareStatement(sql);
      }
      oidStatementComplex = getOidStatementComplexNonArray;
    }
    //type name requested may be schema specific, of the form "{schema}"."typeName",
    //or may check across all schemas where a schema is not specified.
    String fullName = isArray ? pgTypeName.substring(0, pgTypeName.length() - 2) : pgTypeName;
    String schema;
    String name;
    // simple use case
    if (dotIndex == -1) {
      schema = null;
      name = fullName;
    } else {
      if (fullName.startsWith("\"")) {
        if (fullName.endsWith("\"")) {
          String[] parts = fullName.split("\"\\.\"");
          schema = parts.length == 2 ? parts[0] + "\"" : null;
          name = parts.length == 2 ? "\"" + parts[1] : parts[0];
        } else {
          int lastDotIndex = fullName.lastIndexOf('.');
          name = fullName.substring(lastDotIndex + 1);
          schema = fullName.substring(0, lastDotIndex);
        }
      } else {
        schema = fullName.substring(0, dotIndex);
        name = fullName.substring(dotIndex + 1);
      }
    }
    if (schema != null && schema.startsWith("\"") && schema.endsWith("\"")) {
      schema = schema.substring(1, schema.length() - 1);
    } else if (schema != null) {
      schema = schema.toLowerCase(Locale.ROOT);
    }
    if (name.startsWith("\"") && name.endsWith("\"")) {
      name = name.substring(1, name.length() - 1);
    } else {
      name = name.toLowerCase(Locale.ROOT);
    }
    oidStatementComplex.setString(1, name);
    oidStatementComplex.setString(2, schema);
    oidStatementComplex.setBoolean(3, schema == null);
    return oidStatementComplex;
  }
  
  public synchronized int getPGType(String pgTypeName) throws SQLException {
    Integer oid = _pgNameToOid.get(pgTypeName);
    if (oid != null) {
      return oid;
    }

    PreparedStatement oidStatement = getOidStatement(pgTypeName);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) oidStatement).executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    oid = Oid.UNSPECIFIED;
    ResultSet rs = oidStatement.getResultSet();
    if (rs.next()) {
      oid = (int) rs.getLong(1);
      String internalName = rs.getString(2);
      _oidToPgName.put(oid, internalName);
      _pgNameToOid.put(internalName, oid);
    }
    _pgNameToOid.put(pgTypeName, oid);
    rs.close();

    return oid;
  }

  public synchronized String getPGType(int oid) throws SQLException {
    if (oid == Oid.UNSPECIFIED) {
      return null;
    }

    String pgTypeName = _oidToPgName.get(oid);
    if (pgTypeName != null) {
      return pgTypeName;
    }
    if (_getTypeNameByOIDStatement == null) {
      String sql;
      sql = "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname "
              + "FROM pg_catalog.pg_type t "
              + "JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = ?";
      _getTypeNameByOIDStatement = _conn.prepareStatement(sql);
    }

    _getTypeNameByOIDStatement.setInt(1, oid);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) _getTypeNameByOIDStatement).executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    ResultSet rs = _getTypeNameByOIDStatement.getResultSet();
    if (rs.next()) {
      boolean onPath = rs.getBoolean(1);
      String schema = rs.getString(2);
      String name = rs.getString(3);
      if (onPath) {
        pgTypeName = name;
        _pgNameToOid.put(schema + "." + name, oid);
      } else {
        // TODO: escaping !?
        pgTypeName = "\"" + schema + "\".\"" + name + "\"";
        // if all is lowercase add special type info
        // TODO: should probably check for all special chars
        if (schema.equals(schema.toLowerCase()) && schema.indexOf('.') == -1
            && name.equals(name.toLowerCase()) && name.indexOf('.') == -1) {
          _pgNameToOid.put(schema + "." + name, oid);
        }
      }
      _pgNameToOid.put(pgTypeName, oid);
      _oidToPgName.put(oid, pgTypeName);
    }
    rs.close();

    return pgTypeName;
  }

  public int getPGArrayType(String elementTypeName) throws SQLException {
    elementTypeName = getTypeForAlias(elementTypeName);
    int pgType = Oid.UNSPECIFIED;
    for (String newTypeName: new String[] {
            combainStringIfOneQuoted("_", elementTypeName),
            combainStringIfOneQuoted(elementTypeName, "[]")}) {
        pgType = getPGType(newTypeName);
        if (pgType != Oid.UNSPECIFIED) {
          return pgType;
        }
    }
    return pgType;
  }

  private static String combainStringIfOneQuoted(String first, String second) {
    if (first.startsWith("\"")) {
      return first.substring(0, first.length() - 1) + second + "\"";
    }
    if (second.startsWith("\"")) {
      return "\"" + first + second.substring(1);
    }
    return first + second;
  }

  /**
   * Return the oid of the array's base element if it's an array, if not return the provided oid.
   * This doesn't do any database lookups, so it's only useful for the originally provided type
   * mappings. This is fine for it's intended uses where we only have intimate knowledge of types
   * that are already known to the driver.
   *
   * @param oid input oid
   * @return oid of the array's base element or the provided oid (if not array)
   */
  protected synchronized int convertArrayToBaseOid(int oid) {
    Integer i = _pgArrayToPgType.get(oid);
    if (i == null) {
      return oid;
    }
    return i;
  }

  public synchronized char getArrayDelimiter(int oid) throws SQLException {
    if (oid == Oid.UNSPECIFIED) {
      return ',';
    }

    Character delim = _arrayOidToDelimiter.get(oid);
    if (delim != null) {
      return delim;
    }

    if (_getArrayDelimiterStatement == null) {
      String sql;
      sql = "SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e "
            + "WHERE t.oid = ? and t.typelem = e.oid";
      _getArrayDelimiterStatement = _conn.prepareStatement(sql);
    }

    _getArrayDelimiterStatement.setInt(1, oid);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) _getArrayDelimiterStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    ResultSet rs = _getArrayDelimiterStatement.getResultSet();
    if (!rs.next()) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    String s = rs.getString(1);
    delim = s.charAt(0);

    _arrayOidToDelimiter.put(oid, delim);

    rs.close();

    return delim;
  }

  public synchronized int getPGArrayElement(int oid) throws SQLException {
    if (oid == Oid.UNSPECIFIED) {
      return Oid.UNSPECIFIED;
    }

    Integer pgType = _pgArrayToPgType.get(oid);

    if (pgType != null) {
      return pgType;
    }

    if (_getArrayElementOidStatement == null) {
      String sql;
      sql = "SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e ON t.typelem = e.oid "
            + "JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = ?";
      _getArrayElementOidStatement = _conn.prepareStatement(sql);
    }

    _getArrayElementOidStatement.setInt(1, oid);

    // Go through BaseStatement to avoid transaction start.
    if (!((BaseStatement) _getArrayElementOidStatement)
        .executeWithFlags(QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    ResultSet rs = _getArrayElementOidStatement.getResultSet();
    if (!rs.next()) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    pgType = (int) rs.getLong(1);
    boolean onPath = rs.getBoolean(2);
    String schema = rs.getString(3);
    String name = rs.getString(4);
    _pgArrayToPgType.put(oid, pgType);
    _pgNameToOid.put(schema + "." + name, pgType);
    String fullName = "\"" + schema + "\".\"" + name + "\"";
    _pgNameToOid.put(fullName, pgType);
    if (onPath && name.equals(name.toLowerCase())) {
      _oidToPgName.put(pgType, name);
      _pgNameToOid.put(name, pgType);
    } else {
      _oidToPgName.put(pgType, fullName);
    }

    rs.close();

    return pgType;
  }

  public synchronized Class<? extends PGobject> getPGobject(String type) {
    return _pgNameToPgObject.get(type);
  }

  public synchronized String getJavaClass(int oid) throws SQLException {
    String pgTypeName = getPGType(oid);

    String result = _pgNameToJavaClass.get(pgTypeName);
    if (result != null) {
      return result;
    }

    if (getSQLType(pgTypeName) == Types.ARRAY) {
      result = "java.sql.Array";
      _pgNameToJavaClass.put(pgTypeName, result);
    }

    return result;
  }

  public String getTypeForAlias(String alias) {
    String type = typeAliases.get(alias);
    if (type != null) {
      return type;
    }
    if (alias.indexOf('"') == -1) {
      type = typeAliases.get(alias.toLowerCase());
      if (type != null) {
        return type;
      }
    }
    return alias;
  }

  private static ConcurrentHashMap<Integer, Integer> pricisionToInteger = new ConcurrentHashMap<Integer, Integer>();
  static {
      pricisionToInteger.put(Oid.INT1, 3);
      pricisionToInteger.put(Oid.INT2, 5);
      pricisionToInteger.put(Oid.OID, 10);
      pricisionToInteger.put(Oid.INT4, 10);
      pricisionToInteger.put(Oid.INT8, 19);
      // For float4 and float8, we can normally only get 6 and 15
      // significant digits out, but extra_float_digits may raise
      // that number by up to two digits.
      pricisionToInteger.put(Oid.FLOAT4, 8);
      pricisionToInteger.put(Oid.FLOAT8, 17);
      pricisionToInteger.put(Oid.CHAR, 1);
      pricisionToInteger.put(Oid.BOOL, 1);
  }
  public int getPrecision(int oid, int typmod) {
    oid = convertArrayToBaseOid(oid);
    if (pricisionToInteger.containsKey(oid)) {
        return pricisionToInteger.get(oid);
    }
    switch (oid) {
      case Oid.NUMERIC:
        if (typmod == -1) {
          return 0;
        }
        return ((typmod - 4) & 0xFFFF0000) >> 16;

      case Oid.BPCHAR:
      case Oid.VARCHAR:
      case Oid.NVARCHAR2:
        if (typmod == -1) {
          return _unknownLength;
        }
        return typmod - 4;

      // datetime types get the
      // "length in characters of the String representation"
      case Oid.DATE:
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.INTERVAL:
      case Oid.TIMESTAMP:
      case Oid.SMALLDATETIME:
      case Oid.TIMESTAMPTZ:
        return getDisplaySize(oid, typmod);

      case Oid.BIT:
        return typmod;

      case Oid.VARBIT:
        if (typmod == -1) {
          return _unknownLength;
        }
        return typmod;

      case Oid.TEXT:
      case Oid.BYTEA:
      default:
        return _unknownLength;
    }
  }

  public int getScale(int oid, int typmod) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.FLOAT4:
        return 8;
      case Oid.FLOAT8:
        return 17;
      case Oid.NUMERIC:
        if (typmod == -1) {
          return 0;
        }
        return (typmod - 4) & 0xFFFF;
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.TIMESTAMP:
      case Oid.TIMESTAMPTZ:
        if (typmod == -1) {
          return 6;
        }
        return typmod;
      case Oid.INTERVAL:
        if (typmod == -1) {
          return 6;
        }
        return typmod & 0xFFFF;
      default:
        return 0;
    }
  }

  public boolean isCaseSensitive(int oid) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.OID:
      case Oid.INT1:
      case Oid.INT2:
      case Oid.INT4:
      case Oid.INT8:
      case Oid.FLOAT4:
      case Oid.FLOAT8:
      case Oid.NUMERIC:
      case Oid.BOOL:
      case Oid.BIT:
      case Oid.VARBIT:
      case Oid.DATE:
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.TIMESTAMP:
      case Oid.SMALLDATETIME:
      case Oid.TIMESTAMPTZ:
      case Oid.INTERVAL:
        return false;
      default:
        return true;
    }
  }

  public boolean isSigned(int oid) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.INT2:
      case Oid.INT4:
      case Oid.INT8:
      case Oid.FLOAT4:
      case Oid.FLOAT8:
      case Oid.NUMERIC:
        return true;
      default:
        return false;
    }
  }

  private static ConcurrentHashMap<Integer, Integer> oidToInteger = new ConcurrentHashMap<Integer, Integer>();
  static {
      oidToInteger.put(Oid.INT1, 3);
      oidToInteger.put(Oid.INT2, 6); // -32768 to +32767
      oidToInteger.put(Oid.INT4, 11); // -2147483648 to +2147483647
      oidToInteger.put(Oid.OID, 10); // 0 to 4294967295
      oidToInteger.put(Oid.INT8, 20); // -9223372036854775808 to +9223372036854775807
      // varies based upon the extra_float_digits GUC.
      // These values are for the longest possible length.
      oidToInteger.put(Oid.FLOAT4, 15); // sign + 9 digits + decimal point + e + sign + 2 digits
      oidToInteger.put(Oid.FLOAT8, 25); // sign + 18 digits + decimal point + e + sign + 3 digits
      oidToInteger.put(Oid.CHAR, 1);
      oidToInteger.put(Oid.BOOL, 1);
      oidToInteger.put(Oid.DATE, 13); // "4713-01-01 BC" to "01/01/4713 BC" - "31/12/32767"
      oidToInteger.put(Oid.INTERVAL, 49);
  }
  private int getValueOfTime(int oid, int typmod) {
      // Calculate the number of decimal digits + the decimal point.
      int secondSize;
      switch (typmod) {
          case -1:
              secondSize = 6 + 1;
              break;
          case 0:
              secondSize = 0;
              break;
          case 1:
              // Bizarrely SELECT '0:0:0.1'::time(1); returns 2 digits.
              secondSize = 2 + 1;
              break;
          default:
              secondSize = typmod + 1;
              break;
        }

      // We assume the worst case scenario for all of these.
      // time = '00:00:00' = 8
      // date = '5874897-12-31' = 13 (although at large values second precision is lost)
      // date = '294276-11-20' = 12 --enable-integer-datetimes
      // zone = '+11:30' = 6;

      switch (oid) {
          case Oid.TIME:
              return 8 + secondSize;
          case Oid.TIMETZ:
              return 8 + secondSize + 6;
          case Oid.SMALLDATETIME:
              return 13 + 1 + 8;
          case Oid.TIMESTAMP:
              return 13 + 1 + 8 + secondSize;
          case Oid.TIMESTAMPTZ:
          default:
              return 13 + 1 + 8 + secondSize + 6;
        }
  }
  public int getDisplaySize(int oid, int typmod) {
    oid = convertArrayToBaseOid(oid);
    if (oidToInteger.containsKey(oid)) {
        return oidToInteger.get(oid);
    }
    switch (oid) {
      case Oid.TIME:
      case Oid.TIMETZ:
      case Oid.TIMESTAMP:
      case Oid.SMALLDATETIME:
      case Oid.TIMESTAMPTZ:
          return getValueOfTime(oid, typmod);
      case Oid.VARCHAR:
      case Oid.BPCHAR:
        if (typmod == -1) {
          return _unknownLength;
        }
        return typmod - 4;
      case Oid.NUMERIC:
        if (typmod == -1) {
          return 131089; // SELECT LENGTH(pow(10::numeric,131071)); 131071 = 2^17-1
        }
        int precision = (typmod - 4 >> 16) & 0xffff;
        int scale = (typmod - 4) & 0xffff;
        // sign + digits + decimal point (only if we have nonzero scale)
        return 1 + precision + (scale != 0 ? 1 : 0);
      case Oid.BIT:
        return typmod;
      case Oid.VARBIT:
        if (typmod == -1) {
          return _unknownLength;
        }
        return typmod;
      case Oid.TEXT:
      case Oid.BYTEA:
        return _unknownLength;
      default:
        return _unknownLength;
    }
  }

  public int getMaximumPrecision(int oid) {
    oid = convertArrayToBaseOid(oid);
    switch (oid) {
      case Oid.NUMERIC:
        return 1000;
      case Oid.TIME:
      case Oid.TIMETZ:
        // Technically this depends on the --enable-integer-datetimes
        // configure setting. It is 6 with integer and 10 with float.
        return 6;
      case Oid.TIMESTAMP:
      case Oid.SMALLDATETIME:
      case Oid.TIMESTAMPTZ:
      case Oid.INTERVAL:
        return 6;
      case Oid.BPCHAR:
      case Oid.VARCHAR:
        return 10485760;
      case Oid.BIT:
      case Oid.VARBIT:
        return 83886080;
      default:
        return 0;
    }
  }

  public boolean requiresQuoting(int oid) throws SQLException {
    int sqlType = getSQLType(oid);
    return requiresQuotingSqlType(sqlType);
  }

  /**
   * Returns true if particular sqlType requires quoting.
   * This method is used internally by the driver, so it might disappear without notice.
   *
   * @param sqlType sql type as in java.sql.Types
   * @return true if the type requires quoting
   * @throws SQLException if something goes wrong
   */
  public boolean requiresQuotingSqlType(int sqlType) throws SQLException {
    switch (sqlType) {
      case Types.BIGINT:
      case Types.DOUBLE:
      case Types.FLOAT:
      case Types.INTEGER:
      case Types.REAL:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return false;
    }
    return true;
  }
}
