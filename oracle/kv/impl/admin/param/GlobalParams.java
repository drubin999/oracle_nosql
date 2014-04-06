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

package oracle.kv.impl.admin.param;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import oracle.kv.impl.param.DefaultParameter;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;

import com.sleepycat.persist.model.Persistent;

/**
 * GlobalParams store system-wide operational parameters for a KVS instance.
 */
@Persistent
public class GlobalParams implements Serializable {

    private static final long serialVersionUID = 1L;
    private ParameterMap map;

    /**
     * The RMI service name for all SNAs.
     */
    public static final String SNA_SERVICE_NAME = "snaService";

    /**
     * The RMI command service name for AdminService and its test interface
     */
    public static final String COMMAND_SERVICE_NAME = "commandService";
    public static final String COMMAND_SERVICE_TEST_NAME = "commandServiceTest";

    /**
     * The RMI service name for the Admin login interface
     */
    public static final String ADMIN_LOGIN_SERVICE_NAME = "admin:LOGIN";

    /**
     * The RMI service name for the SNA trusted login interface
     */
    public static final String SNA_LOGIN_SERVICE_NAME = "SNA:TRUSTED_LOGIN";

    /* For DPL */
    public GlobalParams() {
    }

    public GlobalParams(ParameterMap map) {
        this.map = map;
    }

    public GlobalParams(String kvsName) {

        map = new ParameterMap(ParameterState.GLOBAL_TYPE,
                               ParameterState.GLOBAL_TYPE);
        map.setParameter(ParameterState.COMMON_STORENAME, kvsName);
    }

    /**
     * Gets the global security parameters. If the parameters are not explictly
     * set in globalParams, default values will be used.
     */
    public ParameterMap getGlobalSecurityPolicies() {
        final EnumSet<ParameterState.Info> set =
            EnumSet.of(ParameterState.Info.GLOBAL,
                       ParameterState.Info.SECURITY);
        final ParameterMap pmap = new ParameterMap();
        for (final ParameterState ps : ParameterState.getMap().values()) {
            if (ps.containsAll(set)) {
                final String pName =
                    DefaultParameter.getDefaultParameter(ps).getName();
                pmap.put(map.getOrDefault(pName));
            }
        }
        return pmap;
    }

    public ParameterMap getMap() {
        return map;
    }

    /**
     * During bootstrap the Admin needs to set the store name.
     */
    public void setKVStoreName(String kvsName) {
        map.setParameter(ParameterState.COMMON_STORENAME, kvsName);
    }

    public String getKVStoreName() {
        return map.get(ParameterState.COMMON_STORENAME).asString();
    }

    /**
     * The value of isLoopback is not valid unless isLoopbackSet() is true.
     */
    public boolean isLoopbackSet() {
        return map.exists(ParameterState.GP_ISLOOPBACK);
    }

    public boolean isLoopback() {
        return map.get(ParameterState.GP_ISLOOPBACK).asBoolean();
    }

    public void setIsLoopback(boolean value) {
        map.setParameter(ParameterState.GP_ISLOOPBACK,
                         Boolean.toString(value));
    }

    /**
     * Accessors for session and login parameters
     */

    public void setSessionTimeout(String value) {
        map.setParameter(ParameterState.GP_SESSION_TIMEOUT, value);
    }

    public long getSessionTimeout() {
        DurationParameter dp = (DurationParameter) map.getOrDefault(
            ParameterState.GP_SESSION_TIMEOUT);
        return dp.getAmount();
    }

    public TimeUnit getSessionTimeoutUnit() {
        DurationParameter dp = (DurationParameter) map.getOrDefault(
            ParameterState.GP_SESSION_TIMEOUT);
        return dp.getUnit();
    }

    public void setSessionExtendAllow(String value) {
        map.setParameter(ParameterState.GP_SESSION_EXTEND_ALLOW, value);
    }

    public boolean getSessionExtendAllow() {
        return map.getOrDefault(ParameterState.GP_SESSION_EXTEND_ALLOW).
            asBoolean();
    }

    public void setAcctErrLockoutThrCnt(String value) {
        map.setParameter(ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT,
                         value);
    }

    public int getAcctErrLockoutThrCount() {
        return map.getOrDefault(
            ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_COUNT).asInt();
    }

    public void setAcctErrLockoutThrInt(String value) {
        map.setParameter(ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL,
                         value);
    }

    public long getAcctErrLockoutThrInt() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL);
        return dp.getAmount();
    }

    public TimeUnit getAcctErrLockoutThrIntUnit() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_THR_INTERVAL);
        return dp.getUnit();
    }

    public void setAcctErrLockoutTimeout(String value) {
        map.setParameter(ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT, value);
    }

    public long getAcctErrLockoutTimeout() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT);
            return dp.getAmount();
        }

    public TimeUnit getAcctErrLockoutTimeoutUnit() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_ACCOUNT_ERR_LOCKOUT_TIMEOUT);
        return dp.getUnit();
    }

    public void setLoginCacheTimeout(String value) {
        map.setParameter(ParameterState.GP_LOGIN_CACHE_TIMEOUT, value);
    }

    public long getLoginCacheTimeout() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_LOGIN_CACHE_TIMEOUT);
            return dp.getAmount();
        }

    public TimeUnit getLoginCacheTimeoutUnit() {
        DurationParameter dp =
            (DurationParameter) map.getOrDefault(
                ParameterState.GP_LOGIN_CACHE_TIMEOUT);
        return dp.getUnit();
    }

    public boolean equals(GlobalParams other) {
        return map.equals(other.getMap());
    }
}
