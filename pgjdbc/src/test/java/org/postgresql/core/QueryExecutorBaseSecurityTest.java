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
