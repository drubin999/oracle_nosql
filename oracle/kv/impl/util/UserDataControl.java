/*-
 *
 *  This file is part of Oracle NoSQL Database
 *  Copyright (C) 2011, 2014 Oracle and/or its affiliates.  All rights reserved.
 *
 *  Oracle NoSQL Database is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, version 3.
 *
 *  Oracle NoSQL Database is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public
 *  License in the LICENSE file along with Oracle NoSQL Database.  If not,
 *  see <http://www.gnu.org/licenses/>.
 *
 *  An active Oracle commercial licensing agreement for this product
 *  supercedes this license.
 *
 *  For more information please contact:
 *
 *  Vice President Legal, Development
 *  Oracle America, Inc.
 *  5OP-10
 *  500 Oracle Parkway
 *  Redwood Shores, CA 94065
 *
 *  or
 *
 *  berkeleydb-info_us@oracle.com
 *
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  [This line intentionally left blank.]
 *  EOF
 *
 */

package oracle.kv.impl.util;

import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.Value;
import oracle.kv.impl.param.ParameterListener;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;

/**
 * This is the locus of control for secure display of user data.
 *
 * KVStore records are represented within the implementation as 
 * - {@link oracle.kv.Key} or {@link oracle.kv.Value}
 * - after passing through to the server side, keys and values are generally
 * represented as byte arrays rather than objects, in order to reduce the cost
 * of serialization and deserialization.
 *
 * Keys and values are meant to be easily viewed as Strings. Keys intentionally
 * provide both toString() and fromString() methods as ways of displaying and
 * creating keys. However, in order to restrict the insecure display of user
 * data, care must be taken to avoid the inadvertent use of Key.toString() and
 * Value.toString().
 *
 * Display Guidelines
 * -------------------
 * - by default, user data is not displayed in server side logs or
 * exceptions. The content of a key or value is replaced with the word
 * "hidden".  It is possible to enable the display of keys or values for
 * debugging purposes.
 *
 * - user data is visible when accessed by utilities whose purpose is to 
 * display records, such as a CLI CRUD utility. Security is implemented at
 * a higher level, through privilege controls for that utility
 *
 * - user data may be visible within client side exception messages which
 * are in direct reaction to api operations. User data is most commonly 
 * added to IllegalArgumentExceptions, in order to explain the problem. 
 * Currently, by default, keys and values are visible. Possible options are
 * to change that default to hidden, to omit the mention of the record on
 * the theory that the target record is self evident in the context of the
 * exception, or to make the record available as a getter on the exception,
 * rather than having the record displayed as part of the message.
 *
 * Implementation Guidelines
 * -------------------------
 * Any display of keys, key ranges, or values in exception messages or to the
 * log should use the static display{Key,Value} methods as gatekeepers, rather
 * than Key.toString(), KeyRange.toString() and Value.toString().
 */
public class UserDataControl {

    /** If true, keys are displayed as the string "hidden" */
    private static boolean HIDE_KEY = true;

    /** If true, value are displayed as the string "hidden" */
    private static boolean HIDE_VALUE = true;

    public static final String HIDDEN = "[hidden]";

    /** Key hiding may be controlled by a kvstore param */
    private static void setKeyHiding(boolean shouldHide) {
        HIDE_KEY = shouldHide;
    }

    /** Value hiding may be controlled by a kvstore param */
    private static void setValueHiding(boolean shouldHide) {
        HIDE_VALUE = shouldHide;
    }

    private static ParameterListener PARAM_SETTER = new ParamSetter();

    /**
     * Depending on configuration, display:
     * - a string representing the hidden key or
     * - the actual key, or
     * - "null" if keyBytes are null.
     */
    public static String displayKey(final byte[] keyBytes) {
        if (keyBytes == null) {
            return "null";
        }
        return HIDE_KEY ? HIDDEN: Key.fromByteArray(keyBytes).toString();
    }

    /**
     * Depending on configuration, display:
     * - a string representing the hidden key or
     * - the actual key, or
     * - "null" if key is null.
     */
    public static String displayKey(final Key key) {
        if (key == null) {
            return "null";
        }
        return HIDE_KEY ? HIDDEN: key.toString();
    }

    /**
     * Depending on configuration, display:
     * - a string representing the hidden keyrange or
     * - the actual keyrange, or
     * - "null" if the keyrange is null.
     */
    public static String displayKeyRange(final KeyRange keyRange) {
        if (keyRange == null) {
            return "null";
        }
        return HIDE_KEY ? HIDDEN: keyRange.toString();
    }

    /**
     * Provide a Value or a byte array representing the value.
     * Depending on configuration, display:
     * - a string representing the hidden value or
     * - the actual value or
     * - "null" if both the value and the valueBytes are null
     */
    public static String displayValue(final Value value, 
                                      final byte[] valueBytes ) {
        if (value == null) {
            if (valueBytes == null) {
                return "null";
            }
            
            /* valueBytes is not null, but value is null */
            return HIDE_VALUE ?  HIDDEN : 
                Value.fromByteArray(valueBytes).toString();
        }
        
        return HIDE_VALUE ? HIDDEN : value.toString();
    }

    public static ParameterListener getParamListener() {
        return PARAM_SETTER;
    }

    private static class ParamSetter implements ParameterListener {

        @Override
        public void newParameters(ParameterMap oldMap, ParameterMap newMap) {
            boolean hideData = newMap.getOrDefault
                    (ParameterState.COMMON_HIDE_USERDATA).asBoolean();
            setKeyHiding(hideData);
            setValueHiding(hideData);
        }
    }
}
