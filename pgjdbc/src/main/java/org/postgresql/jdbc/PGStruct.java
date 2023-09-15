/*
 * Copyright (c) 2004, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.List;
import java.util.Map;

/**
 * @Projecet pgjdbc
 * @Package org.postgresql.jdbc
 * @Class PGStruct
 * @Description: The java bean corresponding to the type type is stored in jdbc
 */
public class PGStruct extends PGobject implements Struct {
    /**
     * The OID of this field.
     */
    private int oid;

    /**
     * A database connection.
     */
    protected BaseConnection conn;

    /**
     * The attr_type_id list corresponding to the attribute in the PGStruct object.
     */
    private List<Integer> attrsSqlTypeList;

    /**
     * The standard_Conforming_Strings property of db.
     */
    boolean standardConformingStrings;

    /**
     * Whether to support escapeLiteral escapes.
     */
    boolean supportsEStringSyntax = true;

    public PGStruct(BaseConnection conn, int oid, String attrsValue) throws SQLException {
        this.conn = conn;
        this.oid = oid;
        this.type = conn.getTypeInfo().getPGType(oid);
        super.value = attrsValue;
        this.attrsSqlTypeList = getAttrsSqlTypeList();
        this.standardConformingStrings = conn.getStandardConformingStrings();
    }

    public PGStruct(BaseConnection conn, int oid, Object[] attributes) throws SQLException {
        this.oid = oid;
        this.conn = conn;
        this.standardConformingStrings = conn.getStandardConformingStrings();
        super.type = conn.getTypeInfo().getPGType(oid);
        super.value = PGStructAttrsConverter.convertAttributes(attributes, standardConformingStrings, supportsEStringSyntax);
        this.attrsSqlTypeList = getAttrsSqlTypeList();
    }

    public int getOid() {
        return this.oid;
    }

    public void setAttrsSqlTypeList(List<Integer> attrsSqlTypeList) {
        this.attrsSqlTypeList = attrsSqlTypeList;
    }

    /**
     * @MethodName:
     * @Params
     * @Return
     * @Exception
     * @Description: According to the oid of the PGStruct object, get the attr_type_id list of the object.
     */
    private List<Integer> getAttrsSqlTypeList() throws SQLException {
        final TypeInfo typeInfo = conn.getTypeInfo();
        return typeInfo.getStructAttributesOid(oid);
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return type;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        if (attrsSqlTypeList == null) {
            attrsSqlTypeList = getAttrsSqlTypeList();
        }
        return PGStructAttrsConverter.parseAttributes(conn, attrsSqlTypeList, value);
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        throw Driver.notImplemented(getClass(), "getAttributes(Map)");
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PGStruct)) {
            return false;
        }

        PGStruct tmp = (PGStruct) obj;
        boolean result = this.toString().equals(tmp.toString());
        if (!result) {
            return false;
        }

        if (this.standardConformingStrings != tmp.standardConformingStrings) {
            return false;
        }

        if (type != null && !type.equals(tmp.type)) {
            return false;
        }

        if (tmp.type != null && !tmp.type.equals(type)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 13;
        hash = 53 * hash + (standardConformingStrings ? 1 : 0);
        hash = 53 * hash + (type != null ? type.hashCode() : 0);
        hash = 53 * hash + (value != null ? value.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        if (value == null) {
            return "NULL";
        }
        return value;
    }
}
