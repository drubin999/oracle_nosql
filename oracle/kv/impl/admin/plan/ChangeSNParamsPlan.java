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

package oracle.kv.impl.admin.plan;

import java.util.concurrent.atomic.AtomicInteger;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams.RNHeapAndCacheSize;
import oracle.kv.impl.admin.plan.task.NewRepNodeParameters;
import oracle.kv.impl.admin.plan.task.StartRepNode;
import oracle.kv.impl.admin.plan.task.StopRepNode;
import oracle.kv.impl.admin.plan.task.WaitForRepNodeState;
import oracle.kv.impl.admin.plan.task.WriteNewParams;
import oracle.kv.impl.admin.plan.task.WriteNewSNParams;
import oracle.kv.impl.mgmt.MgmtUtil;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class ChangeSNParamsPlan extends AbstractPlan {

    private static final long serialVersionUID = 1L;

    protected ParameterMap newParams;

    public ChangeSNParamsPlan(AtomicInteger idGenerator,
                              String name,
                              Planner planner,
                              StorageNodeId snId,
                              ParameterMap newParams) {

        super(idGenerator, name, planner);

        this.newParams = newParams;

        /**
         * Set correct storage node id because this is going to be stored.
         */
        newParams.setParameter(ParameterState.COMMON_SN_ID,
                               Integer.toString(snId.getStorageNodeId()));

        validateParams(planner, snId);
        addTask(new WriteNewSNParams(this, snId, newParams));

        /*
         * If we have changed the capacity, file system percentage, memory
         * setting or numCPUS of this SN, we may have to change the params
         * for any RNs on this SN.
         */
        if (newParams.exists(ParameterState.COMMON_MEMORY_MB) ||
            newParams.exists(ParameterState.SN_RN_HEAP_PERCENT) ||
            newParams.exists(ParameterState.COMMON_CAPACITY) ||
            newParams.exists(ParameterState.COMMON_NUMCPUS)) {
            updateRNParams(snId, newParams);
        }

        /*
         * This is a no-restart plan at this time, we are done.
         */
    }

    /* DPL */
    protected ChangeSNParamsPlan() {
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    void preExecutionSave() {
       /* Nothing to save before execution. */
    }

    @Override
    public String getDefaultName() {
        return "Change Storage Node Params";
    }

    private void validateParams(Planner p, StorageNodeId snId) {
        if (newParams.getName().
            equals(ParameterState.BOOTSTRAP_MOUNT_POINTS)) {
            PlannerAdmin admin = p.getAdmin();
            Parameters parameters = admin.getCurrentParameters();
            String error =
                StorageNodeParams.validateMountMap(newParams, parameters, snId);
            if (error != null) {
                throw new IllegalArgumentException(error);
            }
        }
        if (newParams.getName().equals(ParameterState.SNA_TYPE)) {
            String error = validateMgmtParams(newParams);
            if (error != null) {
                throw new IllegalArgumentException(error);
            }
            /* Let the StorageNodeParams class validate */
            new StorageNodeParams(newParams).validate();
        }
    }

    /**
     * Return a non-null error message if incorrect mgmt param values are
     * present.
     */
    public static String validateMgmtParams(ParameterMap aParams) {
        if (!aParams.exists(ParameterState.COMMON_MGMT_CLASS)) {
            return null;
        }
        Parameter mgmtClass
            = aParams.get(ParameterState.COMMON_MGMT_CLASS);
        if (! MgmtUtil.verifyImplClassName(mgmtClass.asString())) {
            return
                ("The given value " + mgmtClass.asString() +
                 " is not allowed for the parameter " +
                 mgmtClass.getName());
        }
        if (MgmtUtil.MGMT_SNMP_IMPL_CLASS.equals
            (mgmtClass.asString())) {

            Parameter pollPort
                = aParams.get(ParameterState.COMMON_MGMT_POLL_PORT);
            if (pollPort == null || pollPort.asInt() == 0) {
                return
                    ("The parameter " +
                     ParameterState.COMMON_MGMT_POLL_PORT +
                     " must be given when choosing snmp monitoring.");
            }
        }
        return null;
    }

    public StorageNodeParams getNewParams() {
        return new StorageNodeParams(newParams);
    }

    /**
     * Generate tasks to update the JE cache size or JVM args for any RNS on
     * this SN.
     */
    private void updateRNParams(StorageNodeId snId,
                                ParameterMap newMap) {

        PlannerAdmin admin = planner.getAdmin();
        StorageNodeParams snp = admin.getStorageNodeParams(snId);
        ParameterMap policyMap = admin.getCurrentParameters().copyPolicies();

        /* Find the capacity value to use */
        int capacity = snp.getCapacity();
        if (newMap.exists(ParameterState.COMMON_CAPACITY)) {
            capacity = newMap.get(ParameterState.COMMON_CAPACITY).asInt();
        }

        /*
         * Find the number of RNs hosted on this SN; that affects whether we
         * modify the heap value.
         */
        final int numHostedRNs =
            admin.getCurrentTopology().getHostedRepNodeIds(snId).size();

        /* Find the RN heap memory percent to use */
        int rnHeapPercent = snp.getRNHeapPercent();
        if (newMap.exists(ParameterState.SN_RN_HEAP_PERCENT)) {
            rnHeapPercent =
                newMap.get(ParameterState.SN_RN_HEAP_PERCENT).asInt();
        }

        /* Find the memory mb value to use */
        int memoryMB = snp.getMemoryMB();
        if (newMap.exists(ParameterState.COMMON_MEMORY_MB)) {
            memoryMB = newMap.get(ParameterState.COMMON_MEMORY_MB).asInt();
        }

        /* Find the numCPUs value to use */
        int numCPUs = snp.getNumCPUs();
        if (newMap.exists(ParameterState.COMMON_NUMCPUS)) {
            numCPUs = newMap.get(ParameterState.COMMON_NUMCPUS).asInt();
        }

        /* Find the -XX:ParallelGCThread flag to use */
        int gcThreads = StorageNodeParams.calcGCThreads
            (numCPUs, capacity, snp.getGCThreadFloor(),
             snp.getGCThreadThreshold(), snp.getGCThreadPercent());

        for (RepNodeParams rnp :
                 admin.getCurrentParameters().getRepNodeParams()) {
            if (!rnp.getStorageNodeId().equals(snId)) {
                continue;
            }

            RNHeapAndCacheSize heapAndCache =
                StorageNodeParams.calculateRNHeapAndCache
                (policyMap, capacity, numHostedRNs, memoryMB, rnHeapPercent,
                 rnp.getRNCachePercent());
            ParameterMap rnMap = new ParameterMap(ParameterState.REPNODE_TYPE,
                                                  ParameterState.REPNODE_TYPE);

            /*
             * Hang onto the current JVM params in a local variable. We may
             * be making multiple changes to them, if we change both heap and
             * parallel gc threads.
             */
            String currentJavaMisc = rnp.getJavaMiscParams();
            if (rnp.getMaxHeapMB() != heapAndCache.getHeapMB()) {
                /* Set both the -Xms and -Xmx flags */
                currentJavaMisc = rnp.replaceOrRemoveJVMArg
                    (currentJavaMisc, RepNodeParams.XMS_FLAG,
                     heapAndCache.getHeapValAndUnit());
                currentJavaMisc = rnp.replaceOrRemoveJVMArg
                    (currentJavaMisc, RepNodeParams.XMX_FLAG,
                     heapAndCache.getHeapValAndUnit());
                rnMap.setParameter(ParameterState.JVM_MISC, currentJavaMisc);
            }

            if (rnp.getJECacheSize() != heapAndCache.getCacheBytes()) {
                rnMap.setParameter(ParameterState.JE_CACHE_SIZE,
                                   Long.toString(heapAndCache.getCacheBytes()));
            }

            if (gcThreads != 0) {
                /* change only if old and new values don't match */
                String oldGc = RepNodeParams.parseJVMArgsForPrefix
                    (RepNodeParams.PARALLEL_GC_FLAG, currentJavaMisc);
                if (oldGc != null) {
                    if (Integer.parseInt(oldGc) != gcThreads) {
                        currentJavaMisc =
                            rnp.replaceOrRemoveJVMArg(currentJavaMisc,
                                              RepNodeParams.PARALLEL_GC_FLAG,
                                              Integer.toString(gcThreads));
                        rnMap.setParameter
                            (ParameterState.JVM_MISC, currentJavaMisc);
                    }
                }
            }

            if (rnMap.size() == 0) {
                continue;
            }

            RepNodeId rnId = rnp.getRepNodeId();
            addTask(new WriteNewParams(this,
                                       rnMap,
                                       rnId,
                                       snId,
                                       true));

            if (rnMap.hasRestartRequired()) {
                addTask(new StopRepNode(this, snId, rnId, false));
                addTask(new StartRepNode(this, snId, rnId, false));
                addTask(new WaitForRepNodeState(this,
                                                rnId,
                                                ServiceStatus.RUNNING));
            } else {
                addTask(new NewRepNodeParameters(this, rnp.getRepNodeId()));
            }
        }
    }

    @Override
    void stripForDisplay() {
        newParams = null;
    }
}
