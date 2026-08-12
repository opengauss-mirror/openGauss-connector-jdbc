/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.postgresql.core;

import org.postgresql.PGNotification;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Tests security limits for asynchronous notification buffering.
 *
 * @since 2026-08-12
 */
public class QueryExecutorBaseSecurityTest {
    @Test
    public void notificationQueueIsBounded() {
        ArrayList<PGNotification> notifications = new ArrayList<PGNotification>();

        for (int i = 0; i < QueryExecutorBase.MAX_NOTIFICATION_QUEUE_SIZE + 10; i++) {
            QueryExecutorBase.enqueueNotification(notifications, new Notification("n" + i, i));
        }

        Assert.assertEquals(QueryExecutorBase.MAX_NOTIFICATION_QUEUE_SIZE, notifications.size());
        Assert.assertEquals("n10", notifications.get(0).getName());
        Assert.assertEquals("n1033", notifications.get(notifications.size() - 1).getName());
    }
}
