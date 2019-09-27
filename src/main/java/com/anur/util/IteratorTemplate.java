/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.anur.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Anur IjuoKaruKas on 2019/2/26
 *
 * 从 Kafka 直接拿来的IteratorTemplate
 *
 * Transliteration of the iterator template in google collections. To implement an iterator
 * override makeNext and call allDone() when there is no more items
 */
public abstract class IteratorTemplate<T> implements Iterator<T> {

    private State state = State.NOT_READY;

    private T nextItem = null;

    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY; // 需要滚到下一个item，这期间不给next
        if (nextItem == null) {
            throw new IllegalStateException("Expected item but none found.");
        }
        return nextItem;
    }

    public T peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return nextItem;
    }

    public boolean hasNext() {
        if (state == State.FAILED) {
            throw new IllegalStateException("Iterator is in failed state");
        }
        switch (state) {
        case DONE:
            return false;
        case READY:
            return true;
        default:
            return maybeComputeNext();
        }
    }

    protected abstract T makeNext();

    public boolean maybeComputeNext() {
        state = State.FAILED;
        nextItem = makeNext();
        if (state == State.DONE) {
            return false;
        } else {
            state = State.READY;
            return true;
        }
    }

    protected T allDone() {
        state = state.DONE;
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    protected void resetState() {
        state = State.NOT_READY;
    }
}

enum State {
    DONE,
    READY,
    NOT_READY,
    FAILED
}