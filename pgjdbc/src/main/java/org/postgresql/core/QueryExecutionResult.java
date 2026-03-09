/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.core;

/**
 * QueryExecutionResult: the result of a query execution
 *
 * @author shuaikangzhou
 * @since 2025/10/18
 */
public class QueryExecutionResult {
    private int fetchNum = 1;
    private int fetchSize = 0;
    private long size = 120L;
    private boolean isRecovery = false;
    private boolean isLastQuery = false;
    private byte[] resultHash; // statement result hash for evaluation
    private int rowCount; // the number of rows in the result set
    private SnapShot snapShot;

    public QueryExecutionResult() {
        // init resultHash to 0
        resultHash = new byte[16];
    }

    /**
     * updateHash: update the hash value using the XOR algorithm
     *
     * @param hashCode the hash code to be updated
     */
    public void updateHash(byte[] hashCode) {
        if (hashCode == null || hashCode.length != resultHash.length) {
            return;
        }
        for (int i = 0; i < resultHash.length; i++) {
            resultHash[i] ^= hashCode[i];
        }
    }

    /**
     * addHash: add the hash code to the result hash
     *
     * @param hashCode the hash code to be added
     */
    public void addHash(byte[] hashCode) {
        updateHash(hashCode);
        fetchNum++;
    }

    /**
     * getResultHash: get the result hash
     *
     * @return the result hash
     */
    public byte[] getResultHash() {
        return resultHash;
    }

    /**
     * setFetchSize: set the fetch size
     *
     * @param row the fetch size
     */
    public void setFetchSize(int row) {
        fetchSize = row;
    }

    /**
     * getFetchSize: get the fetch size
     *
     * @return the fetch size
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * isQueryResultConsistent: check if the query result is consistent
     *
     * @param otherResult the other query execution result
     * @return true if the query result is consistent, false otherwise
     */
    public boolean isQueryResultConsistent(QueryExecutionResult otherResult) {
        byte[] thisResultHash = getResultHash();
        byte[] otherResultHash = otherResult.getResultHash();
        for (int i = 0; i < thisResultHash.length; i++) {
            if (thisResultHash[i] != otherResultHash[i]) {
                return false;
            }
        }
        if (getRowCount() != otherResult.getRowCount()) {
            return false;
        }
        return true;
    }

    /**
     * getSize: get the size of the query result
     *
     * @return the size of the query result
     */
    public long getSize() {
        return size;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public void updateRowCount(int rowCount) {
        this.rowCount += rowCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setIsRecovery(boolean isRecovery) {
        this.isRecovery = isRecovery;
    }

    public boolean getIsRecovery() {
        return isRecovery;
    }

    public void setIsLastQuery(boolean isLastQuery) {
        this.isLastQuery = isLastQuery;
    }

    public boolean getIsLastQuery() {
        return isLastQuery;
    }

    public void clear() {
        fetchNum = 1;
        rowCount = 0;
        resultHash = new byte[16];
    }

    /**
     * copy: copy the query execution result
     *
     * @return the copied query execution result
     */
    public QueryExecutionResult copy() {
        QueryExecutionResult copy = new QueryExecutionResult();
        copy.fetchNum = fetchNum;
        copy.rowCount = rowCount;
        copy.resultHash = resultHash.clone();
        copy.fetchSize = fetchSize;
        return copy;
    }

    public void setFetchNum(int fetchNum) {
        this.fetchNum = fetchNum;
    }

    public int getFetchNum() {
        return fetchNum;
    }

    public void setSnapShot(SnapShot snapShot) {
        this.snapShot = snapShot;
    }

    public SnapShot getSnapShot() {
        return snapShot;
    }

    /**
     * getXid: get the transaction ID of the current transaction
     *
     * @return the transaction ID of the current transaction
     */
    public long getXid() {
        if (snapShot == null) {
            return 0L;
        }
        return snapShot.xid;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xFF & b);
            if (hex.length() == 1) {
                sb.append("0");
            }
            sb.append(hex);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        if (snapShot == null) {
            return "QueryExecutionResult{}";
        }
        return "QueryExecutionResult{"
                + "xid=" + snapShot.xid
                + ", xmin=" + snapShot.xmin
                + ", xmax=" + snapShot.xmax
                + ", csn=" + snapShot.csn
                + ", fetchSize=" + fetchSize
                + ", fetchNum=" + fetchNum
                + ", takenDuringRecovery=" + snapShot.isTakenDuringRecovery
                + ", timeline=" + snapShot.timeline
                + ", resultHash=" + bytesToHex(resultHash)
                + ", rowCount=" + rowCount
                + '}';
    }

    /**
     * SnapShot: the snapshot of the query execution
     */
    public static class SnapShot {
        /**
         * xmin: the minimum transaction ID that is visible to the current transaction
         */
        public final long xmin;

        /**
         * xmax: the maximum transaction ID that is visible to the current transaction
         */
        public final long xmax;

        /**
         * csn: the commit sequence number of the current transaction
         */
        public final long csn;

        /**
         * isTakenDuringRecovery: whether the snapshot is taken during recovery
         */
        public final boolean isTakenDuringRecovery;

        /**
         * timeline: the timeline ID of the current transaction
         */
        public final int timeline;

        /**
         * xid: the transaction ID of the current transaction
         */
        public final long xid;

        public SnapShot(long xmin, long xmax, long csn, boolean isTakenDuringRecovery, int timeline, long xid) {
            this.xmin = xmin;
            this.xmax = xmax;
            this.csn = csn;
            this.isTakenDuringRecovery = isTakenDuringRecovery;
            this.timeline = timeline;
            this.xid = xid;
        }
    }
}
