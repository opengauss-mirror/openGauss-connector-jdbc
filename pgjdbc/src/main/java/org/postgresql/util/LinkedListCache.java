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

package org.postgresql.util;

import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.util.NoSuchElementException;
import java.util.Iterator;

/**
 * A cache that stores elements in a linked list.
 *
 * @author shuaikangzhou
 * @since 2025-12-18
 *
 * @param <T> the type of elements in the cache
 */
public class LinkedListCache<T extends CanEstimateSize> implements Iterable<T> {
    private static Log LOGGER = Logger.getLogger(LinkedListCache.class.getName());

    private final int maxSizeEntries;
    private final EvictAction<T> onEvict;
    private final long maxSizeBytes;
    private long currentSize = 0L;
    private final Node<T> head;
    private Node<T> tail;
    private int size; // Linked list size

    /**
     * Create a new cache with the specified maximum size.
     *
     * @param maxSizeEntries the maximum number of elements in the cache
     * @param maxSizeBytes the maximum size of the cache in bytes
     * @param onEvict the action to invoke when an element is removed from the cache
     */
    public LinkedListCache(int maxSizeEntries, long maxSizeBytes, EvictAction<T> onEvict) {
        this.maxSizeEntries = maxSizeEntries;
        this.maxSizeBytes = maxSizeBytes;
        this.onEvict = onEvict;
        head = new Node<>(null, null);
        tail = head;
        size = 0;
    }

    /**
     * Append the element to the end of the list.
     *
     * @param element the element to append
     * @return true if the element is appended, false if the cache is full
     */
    public synchronized boolean append(T element) {
        if (size < maxSizeEntries && currentSize + element.getSize() <= maxSizeBytes) {
            Node<T> newNode = new Node<>(element, null);
            currentSize += element.getSize();
            tail.next = newNode;
            tail = newNode;
            size++;
            return true;
        }
        clear();
        return false;
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        Node<T> current = head.next;
        while (current != null) {
            Node<T> next = current.next;
            current.next = null;
            onEvict.evict(current.data);
            current.data = null;
            current = next;
        }
        tail = head;
        head.next = null;
        size = 0;
        currentSize = 0L;
    }

    /**
     * Get the number of elements in the cache.
     *
     * @return The number of elements in the cache.
     */
    public int size() {
        return size;
    }

    /**
     * Check if the cache is empty.
     *
     * @return true if the cache is empty, false otherwise.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Print the elements in the cache.
     */
    public void print() {
        Node<T> current = head.next;
        while (current != null) {
            LOGGER.info(current.data.toString());
            current = current.next;
        }
    }

    /**
     * 1. Iterator traversal (recommended)
     * Supports foreach loop without modifying the original list during traversal.
     * Example usage:
     * for (T element : cache.iterator()) {
     *     // process element
     * }
     *
     * @return an iterator over the elements in the cache
     */
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private Node<T> current = head.next;

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("Cache is empty or traversal completed");
                }
                T data = current.data;
                current = current.next;
                return data;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Traversal iterator does not support remove");
            }
        };
    }

    private static class Node<T> {
        T data;
        Node<T> next;

        Node(T data, Node<T> next) {
            this.data = data;
            this.next = next;
        }
    }

    /**
     * Callback interface for traversing the cache elements.
     *
     * @param <T> the type of elements in the cache
     */
    public interface TraversalCallback<T> {
        /**
         * Invoked when an element is traversed.
         *
         * @param element the element that is traversed
         * @param index the index of the element in the cache
         * @return true if the traversal should continue, false otherwise
         */
        boolean onTraverse(T element, int index);
    }

    /**
     * Action that is invoked when the entry is removed from the cache.
     *
     * @param <Value> type of the cache entry
     */
    public interface EvictAction<T> {
        /**
         * Invoked when an element is removed from the cache.
         *
         * @param value the element that is removed from the cache
         */
        void evict(T value);
    }
}
