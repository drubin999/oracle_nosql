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

package oracle.kv.impl.admin.plan.task;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import oracle.kv.impl.admin.PlannerAdmin;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;
import oracle.kv.impl.admin.plan.MetadataPlan;
import oracle.kv.impl.admin.plan.TablePlanGenerator;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.param.DurationParameter;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.util.Ping;

import com.sleepycat.persist.model.Persistent;

/**
 * Wait for an add a new index on a RepNode. Each node will populate the
 * new index for its shard.
 */
@Persistent
public class WaitForAddIndex extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private /*final*/ MetadataPlan<TableMetadata> plan;
    private /*final*/ RepGroupId groupId;

    private /*final*/ String indexName;
    private /*final*/ String tableName;

    /**
     *
     * @param plan
     * @param groupId ID of the current rep group of the partition
     * will stop.
     */
    public WaitForAddIndex(MetadataPlan<TableMetadata> plan,
                           RepGroupId groupId,
                           String indexName,
                           String tableName) {
        this.plan = plan;
        this.groupId = groupId;
        this.indexName = indexName;
        this.tableName = tableName;
    }

    /* DPL */
    protected WaitForAddIndex() {
    }

    @Override
    public boolean continuePastError() {
        return true;
    }

    @Override
    public Callable<State> getFirstJob(int taskId, ParallelTaskRunner runner) {
        return makeWaitForAddIndexJob(taskId, runner);
    }

    /**
     * @return a wrapper that will invoke a add index job.
     */
    private JobWrapper makeWaitForAddIndexJob(final int taskId,
                                              final ParallelTaskRunner runner) {

        return new JobWrapper(taskId, runner, "add index") {

            @Override
            public NextJob doJob() {
                return waitForAddIndex(taskId, runner);
            }
        };
    }

    /**
     * Query for add index to complete
     */
    private NextJob waitForAddIndex(int taskId, ParallelTaskRunner runner) {
        final AdminParams ap = plan.getAdmin().getParams().getAdminParams();
        RepNodeAdminAPI masterRN = null;
        try {
            masterRN = getMaster();
            if (masterRN == null) {
                /* No master available, try again later. */
                return new NextJob(Task.State.RUNNING,
                                   makeWaitForAddIndexJob(taskId, runner),
                                   ap.getRNFailoverPeriod());
            }

            plan.getLogger().log(Level.INFO,
                                 "Wait for add index {0} for {1}",
                                 new Object[] {indexName, groupId});

            return queryForDone(taskId, runner, ap);

        } catch (RemoteException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeWaitForAddIndexJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());

        } catch (NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeWaitForAddIndexJob(taskId, runner),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * Find the master of the RepGroup.
     * @return null if no master can be found, otherwise the admin interface
     * for the master RN.
     *
     * @throws NotBoundException
     * @throws RemoteException
     */
    private RepNodeAdminAPI getMaster()
        throws RemoteException, NotBoundException {

        final PlannerAdmin admin = plan.getAdmin();
        Topology topology = admin.getCurrentTopology();
        RepNode masterRN = Ping.getMaster(topology, groupId);
        if (masterRN == null) {
            return null;
        }

        RegistryUtils registryUtils =
                new RegistryUtils(topology, admin.getLoginManager());
        return registryUtils.getRepNodeAdmin(masterRN.getResourceId());
    }

    private NextJob queryForDone(int taskId,
                                 ParallelTaskRunner runner,
                                 AdminParams ap) {

        try {
            RepNodeAdminAPI masterRN = getMaster();
            if (masterRN == null) {
                /* No master to talk to, repeat step2 later. */
                return new NextJob(Task.State.RUNNING,
                                   makeDoneQueryJob(taskId, runner, ap),
                                   ap.getRNFailoverPeriod());
            }

            final boolean done = masterRN.addIndexComplete(indexName,
                                                           tableName);

            plan.getLogger().log(Level.INFO,    // TODO -info
                                 "Add index {0} on {1} done={2}",
                                 new Object[] {indexName,
                                               groupId, done});

            return done ? NextJob.END_WITH_SUCCESS :
                          new NextJob(Task.State.RUNNING,
                                      makeDoneQueryJob(taskId, runner, ap),
                                      getCheckIndexTime(ap));
        } catch (RemoteException e) {
            /* RMI problem, try again later. */
            return new NextJob(Task.State.RUNNING,
                               makeDoneQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        } catch (NotBoundException e) {
            /* RMI problem, try step 1 again later. */
            return new NextJob(Task.State.RUNNING,
                               makeDoneQueryJob(taskId, runner, ap),
                               ap.getServiceUnreachablePeriod());
        }
    }

    /**
     * @return a wrapper that will invoke a done query.
     */
    private JobWrapper makeDoneQueryJob(final int taskId,
                                        final ParallelTaskRunner runner,
                                        final AdminParams ap) {
        return new JobWrapper(taskId, runner, "query add index done") {
            @Override
            public NextJob doJob() {
                return queryForDone(taskId, runner, ap);
            }
        };
    }

    private DurationParameter getCheckIndexTime(AdminParams ap) {
        return ap.getCheckAddIndexPeriod();
    }

    @Override
    public String toString() {
        return TablePlanGenerator.makeName("WaitForAddIndex", tableName,
                                           indexName);
    }
}
