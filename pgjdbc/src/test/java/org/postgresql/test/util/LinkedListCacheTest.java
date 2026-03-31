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

package org.postgresql.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.postgresql.util.CanEstimateSize;
import org.postgresql.util.LinkedListCache;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests {@link org.postgresql.util.LinkedListCache}.
 *
 * @author shuaikangzhou
 * @since 2025-12-18
 */
public class LinkedListCacheTest {
    private List<TestEntry> evictedEntries;
    private LinkedListCache<TestEntry> cache;

    @Before
    public void setUp() {
        evictedEntries = new ArrayList<>();
        cache = new LinkedListCache<TestEntry>(5, 1000, new LinkedListCache.EvictAction<TestEntry>() {
            @Override
            public void evict(TestEntry value) {
                evictedEntries.add(value);
            }
        });
    }

    @Test
    public void testAppendSuccess() {
        TestEntry entry1 = new TestEntry(1, 100);
        TestEntry entry2 = new TestEntry(2, 200);

        assertTrue(cache.append(entry1));
        assertEquals(1, cache.size());

        assertTrue(cache.append(entry2));
        assertEquals(2, cache.size());
    }

    @Test
    public void testAppendWhenFullByEntries() {
        TestEntry entry1 = new TestEntry(1, 10);
        TestEntry entry2 = new TestEntry(2, 10);
        TestEntry entry3 = new TestEntry(3, 10);
        TestEntry entry4 = new TestEntry(4, 10);
        TestEntry entry5 = new TestEntry(5, 10);
        TestEntry entry6 = new TestEntry(6, 10);

        assertTrue(cache.append(entry1));
        assertTrue(cache.append(entry2));
        assertTrue(cache.append(entry3));
        assertTrue(cache.append(entry4));
        assertTrue(cache.append(entry5));
        assertEquals(5, cache.size());

        assertFalse(cache.append(entry6));
        assertEquals(0, cache.size());
        assertEquals(5, evictedEntries.size());
    }

    @Test
    public void testAppendWhenFullByBytes() {
        LinkedListCache<TestEntry> smallCache = new LinkedListCache<TestEntry>(10, 500,
            new LinkedListCache.EvictAction<TestEntry>() {
                @Override
                public void evict(TestEntry value) {
                    evictedEntries.add(value);
                }
            });

        TestEntry entry1 = new TestEntry(1, 200);
        TestEntry entry2 = new TestEntry(2, 200);
        TestEntry entry3 = new TestEntry(3, 200);

        assertTrue(smallCache.append(entry1));
        assertTrue(smallCache.append(entry2));

        assertFalse(smallCache.append(entry3));
        assertEquals(0, smallCache.size());
        assertEquals(2, evictedEntries.size());
    }

    @Test
    public void testClear() {
        TestEntry entry1 = new TestEntry(1, 100);
        TestEntry entry2 = new TestEntry(2, 200);

        cache.append(entry1);
        cache.append(entry2);
        assertEquals(2, cache.size());

        cache.clear();
        assertEquals(0, cache.size());
        assertTrue(cache.isEmpty());
        assertEquals(2, evictedEntries.size());
    }

    @Test
    public void testClearEmptyCache() {
        cache.clear();
        assertEquals(0, cache.size());
        assertEquals(0, evictedEntries.size());
    }

    @Test
    public void testSize() {
        assertEquals(0, cache.size());

        cache.append(new TestEntry(1, 100));
        assertEquals(1, cache.size());

        cache.append(new TestEntry(2, 100));
        assertEquals(2, cache.size());

        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(cache.isEmpty());

        cache.append(new TestEntry(1, 100));
        assertFalse(cache.isEmpty());

        cache.clear();
        assertTrue(cache.isEmpty());
    }

    @Test
    public void testIterator() {
        TestEntry entry1 = new TestEntry(1, 100);
        TestEntry entry2 = new TestEntry(2, 200);
        TestEntry entry3 = new TestEntry(3, 300);

        cache.append(entry1);
        cache.append(entry2);
        cache.append(entry3);

        List<TestEntry> iteratedEntries = new ArrayList<>();
        for (TestEntry entry : cache) {
            iteratedEntries.add(entry);
        }

        assertEquals(3, iteratedEntries.size());
        assertEquals(entry1, iteratedEntries.get(0));
        assertEquals(entry2, iteratedEntries.get(1));
        assertEquals(entry3, iteratedEntries.get(2));
    }

    @Test
    public void testIteratorEmptyCache() {
        int count = 0;
        for (TestEntry entry : cache) {
            count++;
        }
        assertEquals(0, count);
    }

    @Test
    public void testIteratorHasNext() {
        TestEntry entry1 = new TestEntry(1, 100);
        cache.append(entry1);

        java.util.Iterator<TestEntry> iterator = cache.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(entry1, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testIteratorNextOnEmpty() {
        java.util.Iterator<TestEntry> iterator = cache.iterator();
        iterator.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorRemove() {
        cache.append(new TestEntry(1, 100));
        java.util.Iterator<TestEntry> iterator = cache.iterator();
        iterator.remove();
    }

    @Test
    public void testEvictActionCalled() {
        TestEntry entry1 = new TestEntry(1, 100);
        TestEntry entry2 = new TestEntry(2, 200);

        cache.append(entry1);
        cache.append(entry2);
        cache.clear();

        assertEquals(2, evictedEntries.size());
        assertEquals(entry1, evictedEntries.get(0));
        assertEquals(entry2, evictedEntries.get(1));
    }

    @Test
    public void testMultipleClears() {
        cache.append(new TestEntry(1, 100));
        cache.clear();
        assertEquals(0, cache.size());

        cache.append(new TestEntry(2, 200));
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testAppendAfterClear() {
        TestEntry entry1 = new TestEntry(1, 100);
        cache.append(entry1);
        cache.clear();

        TestEntry entry2 = new TestEntry(2, 200);
        assertTrue(cache.append(entry2));
        assertEquals(1, cache.size());
    }

    @Test
    public void testLargeCache() {
        LinkedListCache<TestEntry> largeCache = new LinkedListCache<TestEntry>(1000, 1000000,
            new LinkedListCache.EvictAction<TestEntry>() {
                @Override
                public void evict(TestEntry value) {
                    evictedEntries.add(value);
                }
            });

        for (int i = 0; i < 100; i++) {
            assertTrue(largeCache.append(new TestEntry(i, 100)));
        }
        assertEquals(100, largeCache.size());
    }

    @Test
    public void testZeroMaxSize() {
        LinkedListCache<TestEntry> zeroCache = new LinkedListCache<TestEntry>(0, 0,
            new LinkedListCache.EvictAction<TestEntry>() {
                @Override
                public void evict(TestEntry value) {
                    evictedEntries.add(value);
                }
            });

        assertFalse(zeroCache.append(new TestEntry(1, 100)));
        assertEquals(0, zeroCache.size());
    }

    @Test
    public void testIteratorDoesNotModifyCache() {
        TestEntry entry1 = new TestEntry(1, 100);
        TestEntry entry2 = new TestEntry(2, 200);

        cache.append(entry1);
        cache.append(entry2);

        int count = 0;
        for (TestEntry entry : cache) {
            count++;
        }

        assertEquals(2, count);
        assertEquals(2, cache.size());
    }

    private static class TestEntry implements CanEstimateSize {
        private final int id;
        private final int size;

        TestEntry(int id, int size) {
            this.id = id;
            this.size = size;
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public String toString() {
            return "TestEntry{id=" + id + ", size=" + size + '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TestEntry that = (TestEntry) obj;
            return id == that.id && size == that.size;
        }

        @Override
        public int hashCode() {
            return 31 * id + size;
        }
    }
}
