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
package oracle.kv.impl.security.login;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;

/**
 * ParamTopoResolver provides an implementation of TopologyResolver that
 * resolves based on Admin Parameters.
 */
public class ParamTopoResolver implements TopologyResolver {

    private final ParamsHandle paramsHandle;
    private Logger logger;

    public interface ParamsHandle {
        Parameters getParameters();
    }

    public static class ParamsHandleImpl implements ParamsHandle {
        private volatile Parameters params;

        public ParamsHandleImpl(Parameters initialParams) {
            params = initialParams;
        }

        @Override
        public Parameters getParameters() {
            return params;
        }

        public void setParameters(Parameters newParams) {
            this.params = newParams;
        }
    }

    /**
     * Creates a resolver based on a Parameters.
     * @param paramsHandle must not be null
     */
    public ParamTopoResolver(ParamsHandle paramsHandle, Logger logger) {
        this.paramsHandle = paramsHandle;
        this.logger = logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Resolve a ResourceID to its SNInfo.
     */
    @Override
    public SNInfo getStorageNode(ResourceId target) {

        if (target instanceof AdminId) {
            return getStorageNode((AdminId) target);
        }

        if (target instanceof RepNodeId) {
            return getStorageNode((RepNodeId) target);
        }

        if (target instanceof StorageNodeId) {
            return getStorageNode((StorageNodeId) target);
        }

        logger.info("ParamTopoResolver: unable to resolve target type: " +
                    target.getType());

        return null;
    }

    private SNInfo getStorageNode(AdminId target) {

        final Parameters params = paramsHandle.getParameters();
        if (params == null) {
            logger.info("ParamTopoResolver: unable to resolve AdminId: " +
                        target + " with null Parameters");
            return null;
        }

        final AdminParams ap = params.get(target);
        if (ap == null) {
            logger.info("ParamTopoResolver: unable to resolve AdminId: " +
                        target + " with null AdminParams");
            return null;
        }

        final StorageNodeId snid = ap.getStorageNodeId();
        final StorageNodeParams snp = params.get(snid);
        if (snp == null) {
            throw new IllegalStateException(
                "StorageNode " + snid + " was not found.");
        }

        logger.fine("ParamTopoResolver: Successfully resolved AdminId: " +
                    target);

        return new SNInfo(snp.getHostname(), snp.getRegistryPort(), snid);
    }

    private SNInfo getStorageNode(StorageNodeId target) {

        final Parameters params = paramsHandle.getParameters();
        if (params == null) {
            logger.info("ParamTopoResolver: unable to resolve SnId: " +
                        target + " with null Parameters");
            return null;
        }

        final StorageNodeParams snp = params.get(target);
        if (snp == null) {
            logger.info("ParamTopoResolver: unable to resolve SnId: " +
                        target + " with null StorageParams");
            return null;
        }

        final StorageNodeId snid = snp.getStorageNodeId();

        logger.fine("ParamTopoResolver: successfully resolved SnId: " + target);

        return new SNInfo(snp.getHostname(), snp.getRegistryPort(), snid);
    }

    private SNInfo getStorageNode(RepNodeId target) {

        final Parameters params = paramsHandle.getParameters();
        if (params == null) {
            logger.info("ParamTopoResolver: unable to resolve RnId: " +
                        target + " with null Parameters");
            return null;
        }

        final RepNodeParams rnp = params.get(target);
        if (rnp == null) {
            logger.info("ParamTopoResolver: unable to resolve RnId: " +
                        target + " with null RepNodeParams");
            return null;
        }

        final StorageNodeId snid = rnp.getStorageNodeId();
        final StorageNodeParams snp = params.get(snid);
        if (snp == null) {
            throw new IllegalStateException(
                "StorageNode " + snid + " was not found.");
        }

        logger.fine("ParamTopoResolver: successfully resolved RnId: " + target);

        return new SNInfo(snp.getHostname(), snp.getRegistryPort(), snid);
    }

    @Override
    public List<RepNodeId> listRepNodeIds(int maxReturn) {
        final Parameters params = paramsHandle.getParameters();
        if (params == null) {
            return null;
        }

        final List<RepNodeId> rnList = new ArrayList<RepNodeId>();
        for (RepNodeParams rnp : params.getRepNodeParams()) {
            if (rnList.size() >= maxReturn) {
                break;
            }
            rnList.add(rnp.getRepNodeId());
        }

        return rnList;
    }
}
