/*
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
