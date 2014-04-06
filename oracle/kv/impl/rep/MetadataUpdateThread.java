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

package oracle.kv.impl.rep;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.rgstate.UpdateThread;
import oracle.kv.impl.fault.EnvironmentTimeoutException;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

/**
 * Thread to update the RNs metadata.
 */
public class MetadataUpdateThread extends UpdateThread {

    private final RepNode repNode;

    /**
     * The minimum number of threads used by the pool. This minimum number
     * permits progress in the presence of a few bad network connections.
     */
    private static final int MIN_POOL_SIZE = 6;

    /**
     *  The time interval between executions of a concurrent update pass.
     */
    private static final int UPDATE_THREAD_PERIOD_MS = 2000;

    /**
     * Used to ensure that there is only one pull request running at one time.
     */
    private final Map<MetadataType, Semaphore> pullsInProgress =
            new EnumMap<MetadataType, Semaphore>(MetadataType.class);

    /**
     * Next group to update. This starts at our own group and loops through all
     * of the groups.
     */
    private RepGroupId nextGroup;

    /**
     * Creates the RN state update thread.
     *
     * @param requestDispatcher the request dispatcher associated with this
     * thread
     *
     * @param repNode the rep node
     *
     * @param logger the logger used by the update threads
     */
    public MetadataUpdateThread(RequestDispatcher requestDispatcher,
                                RepNode repNode,
                                final Logger logger) {
        super(requestDispatcher, UPDATE_THREAD_PERIOD_MS,
              requestDispatcher.getExceptionHandler(), logger);
        this.repNode = repNode;
        nextGroup = new RepGroupId(repNode.getRepNodeId().getGroupId());
        for (final MetadataType type : MetadataType.values()) {
            pullsInProgress.put(type, new Semaphore(1));
        }
    }

    /**
     * Updates metadata for one group.
     */
    @Override
    protected void doUpdate() {

        /* Select a group to query */
        final Topology topo = repNode.getTopology();

        if (topo == null) {
            return; /* Not yet initialized */
        }
        /*
         * TODO - This assumes that group IDs are contiguous, starting at 1.
         * When store contraction is implemented and group IDs can contain
         * holes the code will need to be modified.
         */
        if (topo.get(nextGroup) == null) {
            nextGroup = new RepGroupId(1);
        }

        final Collection<RepNodeState> rns = getRNs(nextGroup);

        threadPool.setMaximumPoolSize(Math.max(MIN_POOL_SIZE,
                                               rns.size()/10));
        for (RepNodeState rnState : rns) {
            if (shutdown.get()) {
                return;
            }

            /* Skip ourselves */
            if (rnState.getRepNodeId().equals(repNode.getRepNodeId())) {
                continue;
            }

            if (needsResolution(rnState)) {
                continue;
            }

            /* Check all types of metadata except Topology */
            for (final MetadataType checkType : MetadataType.values()) {
                if (checkType != MetadataType.TOPOLOGY) {
                    Exception exception = null;
                    try {
                        checkMetadata(checkType, rnState);
                    } catch (EnvironmentTimeoutException ete) {
                        /* Timeout in obtaining environment handler */
                        exception = ete;
                    } catch (IllegalStateException ise) {
                        /* May be accessing a closed environment */
                        exception = ise;
                    }

                    /*
                     * If shutdown, simply return, even if there was an
                     * exception
                     */
                    if (shutdown.get()) {
                        return;
                    }
                    
                    /*
                     * Skip this round of updates if environment issue is found.
                     */
                    if (exception != null) {
                        logger.log(Level.INFO,
                                   "Failed to read metadata from environment", 
                                   exception);
                        return;
                    }
                }
            }
        }
        nextGroup = new RepGroupId(nextGroup.getGroupId() + 1);
    }

    /**
     * Checks if the specified metadata type on the RN represented by rnState
     * needs updating. Also checks if this node needs updating.
     * 
     * @param type the metadata type to check
     * @param rnState the RN state
     */
    private void checkMetadata(MetadataType type, RepNodeState rnState) {
        final Metadata<?> md = repNode.getMetadata(type);

        final int localSeqNum = (md == null) ? Metadata.EMPTY_SEQUENCE_NUMBER :
                                               md.getSequenceNumber();
        final int rnSeqNum = rnState.getSeqNum(type);

        /*
         * If we have non-empty metadata that is newer then the target,
         * push it. Otherwise, if they have newer MD pull it.
         */
        if ((localSeqNum != Metadata.EMPTY_SEQUENCE_NUMBER) &&
            (rnSeqNum < localSeqNum)) {
            threadPool.execute(new PushMetadata(rnState, md));
        } else if (rnSeqNum > localSeqNum) {
            threadPool.execute(new PullMetadata(rnState, type));
        }
    }

    /**
     * Pull metadata from a RN.
     */
    private class PullMetadata implements Runnable {

        private final RepNodeState rnState;

        /* The metadata type to be pulled. */
        private final MetadataType type;

        public PullMetadata(RepNodeState rnState, MetadataType type) {
            this.rnState = rnState;
            this.type = type;
        }

        @Override
        public void run() {
            boolean success = false;
            Exception exception = null;

            int localSeqNum;
            try {
                localSeqNum = repNode.getMetadataSeqNum(type);
            } catch (Exception e) {
                /* Exception found in getting the seqNum, skip the pull. */
                logger.log(Level.INFO,
                           "Failed to read metadata sequence number: {0}", e);
                return;
            }

            /* If there has been an update, skip */
            if (localSeqNum >= rnState.getSeqNum(type)) {
                return;
            }

            if (!pullsInProgress.get(type).tryAcquire()) {
                /* A pull is already in progress. */
                return;
            }

            try {
                final RegistryUtils regUtils = requestDispatcher.getRegUtils();
                if (regUtils == null) {

                    /*
                     * The request dispatcher has not initialized itself as
                     * yet. Retry later.
                     */
                    return;
                }
                final RepNodeAdminAPI rnAdmin =
                            regUtils.getRepNodeAdmin(rnState.getRepNodeId());

                final MetadataInfo info =
                                        rnAdmin.getMetadata(type, localSeqNum);

                if (!info.isEmpty()) {
                    repNode.updateMetadata(info);
                    success = true;
                    return;
                }
                final Metadata<?> md = rnAdmin.getMetadata(type);
                if (md != null) {
                    success = repNode.updateMetadata(md);
                }

            } catch (Exception e) {
                exception = e;
            } finally {
                pullsInProgress.get(type).release();
                if (success) {
                    logger.log(Level.INFO,
                               "Pulled {0} from {1}, updated to: {2}",
                               new Object[]{type, rnState.getRepNodeId(),
                                            repNode.getMetadata(type)});
                } else {
                    logOnFailure(rnState.getRepNodeId(),
                                 exception,
                                 " metadata " + type + " pull from: " +
                                 rnState.getRepNodeId() +
                                 " for seqNum:" + rnState.getSeqNum(type));
                }
            }
        }
    }

    /**
     * Task used to push metadata changes to a RN. Incremental changes are
     * sent if available, otherwise, the entire metadata is pushed.
     */
    private class PushMetadata implements Runnable {

        /* The RN to which the metadata changes are to be pushed. */
        private final RepNodeState rnState;

        /* The topology to be pushed. */
        private final Metadata<?> metadata;

        /* The original metadata sequence number of the targer RN. */
        private int rnSeqNum;

        PushMetadata(RepNodeState rns, Metadata<?> metadata) {
            super();

            this.rnState = rns;
            rnSeqNum = rnState.getSeqNum(metadata.getType());

            /*
             * Optimistically update the sequence number, so no new updates are
             * queued while the request is in progress. It will be reverted in
             * the run method below if the update fails.
             */
            rnState.updateSeqNum(metadata.getType(),
                                 metadata.getSequenceNumber());
            this.metadata = metadata;
        }

        @Override
        public void run() {

            String changesMsg = "Unknown failure";
            boolean success = false;
            Exception exception = null;

            try {

                final RegistryUtils regUtils = requestDispatcher.getRegUtils();
                if (regUtils == null) {
                    /*
                     * The request dispatcher has not initialized itself as
                     * yet. Retry later.
                     */
                    return;
                }
                final MetadataInfo info = metadata.getChangeInfo(rnSeqNum);

                final RepNodeId targetRNId = rnState.getRepNodeId();
                final RepNodeAdminAPI rnAdmin =
                                        regUtils.getRepNodeAdmin(targetRNId);

                if (info.isEmpty()) {
                    changesMsg =  "entire " + metadata + " to " + targetRNId +
                                  " updating from seq#: " + rnSeqNum;
                    rnAdmin.updateMetadata(metadata);
                    rnSeqNum = metadata.getSequenceNumber();
                    success = true;
                    return;
                }
                rnSeqNum = rnAdmin.updateMetadata(info);

                if (rnSeqNum >= metadata.getSequenceNumber()) {
                    changesMsg = metadata.getType() + " metadata changes [" +
                                 info + "] to node: " + targetRNId;
                    success = true;
                    return;
                }
                changesMsg = "Push to be retried for rn: " + targetRNId +
                             " Target metadata seq number:" + rnSeqNum +
                             " Local metadata seq number:" +
                             metadata.getSequenceNumber();
            } catch (Exception e) {
                exception = e;
            } finally {

                /* Set the state to the actual value returned from the RN */
                rnState.resetSeqNum(metadata.getType(), rnSeqNum);

                if (success) {
                    logger.log(Level.INFO, "Pushed {0}", changesMsg);   // TODO - FINE
                } else {
                    logOnFailure(rnState.getRepNodeId(),
                                 exception, "Failed pushing " +  changesMsg);
                }
            }
        }
    }
}
