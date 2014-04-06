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

package oracle.kv.impl.admin;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.StorageNodePool;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.admin.plan.PlanStateChange;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.api.avro.AvroDdl;
import oracle.kv.impl.api.avro.AvroSchemaMetadata;
import oracle.kv.impl.api.table.FieldMap;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.mgmt.AdminStatusReceiver;
import oracle.kv.impl.monitor.Tracker.RetrievedEvents;
import oracle.kv.impl.monitor.TrackerListener;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.KVStoreRole;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureR2Method;
import oracle.kv.impl.security.metadata.KVStoreUser.UserDescription;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.test.RemoteTestInterface;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.VersionedRemoteImpl;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.util.DbBackup;

@SecureAPI
public class CommandServiceImpl
    extends VersionedRemoteImpl implements CommandService {

    private final AdminService aservice;

    private RemoteTestInterface rti;
    private static final String TEST_INTERFACE_NAME =
        "oracle.kv.impl.admin.CommandServiceTestInterface";

    /**
     * A cached copy of the Admin.  This is null until configure() is
     * called.
     */
    private Admin admin;

    private DbBackup dbBackup;

    public CommandServiceImpl(AdminService aservice) {
        this.aservice = aservice;
        admin = aservice.getAdmin();
        startTestInterface();
    }

    /**
     * Can the test interface be created?
     */
    private void startTestInterface() {
        try {
            final Class<?> cl = Class.forName(TEST_INTERFACE_NAME);
            final Constructor<?> c = cl.getConstructor(aservice.getClass());
            rti = (RemoteTestInterface) c.newInstance(aservice);
            rti.start(SerialVersion.CURRENT);
        } catch (Exception ignored) /* CHECKSTYLE:OFF */ {
        }/* CHECKSTYLE:ON */
    }

    @Override
    @SecureR2Method
    public synchronized ServiceStatus ping(short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Return the service status associated with the CommandService.  If the
     * service is up, it will return RUNNING.  Otherwise it won't respond
     * at all.  This is just a way for the client to ask if the service is
     * running.
     */
    @Override
    @PublicMethod
    public synchronized ServiceStatus ping(AuthContext authCtx,
                                           short serialVersion) {
        return ServiceStatus.RUNNING;
    }

    @Override
    @SecureR2Method
    public List<String> getStorageNodePoolNames(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public List<String> getStorageNodePoolNames(AuthContext authCtx,
                                                short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<String>>() {

            @Override
            public List<String> execute() {
                requireConfigured();

                final Parameters p = admin.getCurrentParameters();

                final List<String> names =
                    new ArrayList<String>(p.getStorageNodePoolNames());
                Collections.sort(names);
                return names;
            }
        });
    }

    @Override
    @SecureR2Method
    public void addStorageNodePool(final String name, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void addStorageNodePool(final String name,
                                   AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
            @Override

            public void execute() {
                requireConfigured();
                admin.addStorageNodePool(name);
            }
        });
    }

    @Override
    @SecureR2Method
    public void removeStorageNodePool(final String name, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void removeStorageNodePool(final String name, AuthContext authCtx,
                                      short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.removeStorageNodePool(name);
            }
        });
    }

    @Override
    @SecureR2Method
    public List<StorageNodeId> getStorageNodePoolIds(final String name,
                                                     short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public List<StorageNodeId> getStorageNodePoolIds(final String name,
                                                     AuthContext authCtx,
                                                     short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<StorageNodeId>>() {
            @Override

            public List<StorageNodeId> execute() {
                requireConfigured();

                final Parameters p = admin.getCurrentParameters();

                final StorageNodePool pool = p.getStorageNodePool(name);
                if (pool == null) {
                    throw new IllegalCommandException
                        ("No such Storage Node Pool: " + name);
                }

                return pool.getList();
            }
        });
    }

    @Override
    @SecureR2Method
    public void addStorageNodeToPool(final String name,
                                     final StorageNodeId snId,
                                     short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void addStorageNodeToPool(final String name,
                                     final StorageNodeId snId,
                                     AuthContext authCtx,
                                     short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.addStorageNodeToPool(name, snId);
            }
        });
    }

    @Override
    @SecureR2Method
    public void replaceStorageNodePool(final String name,
                                       final List<StorageNodeId> ids,
                                       final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void replaceStorageNodePool(final String name,
                                       final List<StorageNodeId> ids,
                                       AuthContext authCtx,
                                       final short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.replaceStorageNodePool(name, ids);
            }
        });
    }

    @Override
    @SecureR2Method
    public String createTopology(final String candidateName,
                                 final String snPoolName,
                                 final int numPartitions,
                                 short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String createTopology(final String candidateName,
                                 final String snPoolName,
                                 final int numPartitions,
                                 AuthContext authCtx,
                                 short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.createTopoCandidate(candidateName, snPoolName,
                                                 numPartitions);
            }
        });
    }

    @Override
    @SecureR2Method
    public String copyCurrentTopology(final String candidateName,
                                      short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String copyCurrentTopology(final String candidateName,
                                      AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                admin.addTopoCandidate(candidateName,
                                       admin.getCurrentTopology());
                return "Created " + candidateName;
            }
        });
    }

    @Override
    @SecureR2Method
    public String copyTopology(final String sourceCandidateName,
                               final String targetCandidateName,
                               short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String copyTopology(final String sourceCandidateName,
                               final String targetCandidateName,
                               AuthContext authCtx,
                               short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                admin.addTopoCandidate(targetCandidateName,
                                       sourceCandidateName);
                return "Created " + targetCandidateName;
            }
        });
    }

    @Override
    @SecureR2Method
    public String validateTopology(final String candidateName,
                                   short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String validateTopology(final String candidateName,
                                   AuthContext authCtx,
                                   short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.validateTopology(candidateName);
            }
        });
    }

    @Override
    @SecureR2Method
    public List<String> listTopologies(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public List<String> listTopologies(AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<String>>() {

            @Override
            public List<String> execute() {
                requireConfigured();
                return admin.listTopoCandidates();
            }
        });
    }

    @Override
    @SecureR2Method
    public String deleteTopology(final String candidateName,
                                 short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String deleteTopology(final String candidateName,
                                 AuthContext authCtx,
                                 short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                admin.deleteTopoCandidate(candidateName);
                return "Removed " + candidateName;
            }
        });
    }

    @Override
    @SecureR2Method
    public String rebalanceTopology(final String candidateName,
                                    final String snPoolName,
                                    final DatacenterId dcId,
                                    short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String rebalanceTopology(final String candidateName,
                                    final String snPoolName,
                                    final DatacenterId dcId,
                                    AuthContext authCtx,
                                    short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.rebalanceTopology(candidateName, snPoolName,
                                               dcId);
            }
        });
    }

    @Override
    @SecureR2Method
    public String changeRepFactor(final String candidateName,
                                  final String snPoolName,
                                  final DatacenterId dcId,
                                  final int repFactor,
                                  short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String changeRepFactor(final String candidateName,
                                  final String snPoolName,
                                  final DatacenterId dcId,
                                  final int repFactor,
                                  AuthContext authCtx,
                                  short serialVersion) {
        return aservice.getFaultHandler().execute
        (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.changeRepFactor(candidateName, snPoolName,
                                             dcId, repFactor);
            }
        });
    }

    @Override
    @SecureR2Method
    public String redistributeTopology(final String candidateName,
                                       final String snPoolName,
                                       short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String redistributeTopology(final String candidateName,
                                       final String snPoolName,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.redistributeTopology(candidateName, snPoolName);
            }
        });
    }

    @Override
    @SecureR2Method
    public String moveRN(final String candidateName,
                         final RepNodeId rnId,
                         final StorageNodeId snId,
                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String moveRN(final String candidateName,
                         final RepNodeId rnId,
                         final StorageNodeId snId,
                         AuthContext authCtx,
                         short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.moveRN(candidateName, rnId, snId);
            }
       });
    }

    @Override
    @SecureR2Method
    public String preview(final String targetTopoName,
                          final String startTopoName,
                          final boolean verbose,
                          short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String preview(final String targetTopoName,
                          final String startTopoName,
                          final boolean verbose,
                          AuthContext authCtx,
                          short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.previewTopology(targetTopoName, startTopoName,
                                             verbose);
        }
    });
    }

    @Override
    @SecureR2Method
    public List<ParameterMap> getAdmins(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public List<ParameterMap> getAdmins(AuthContext authCtx,
                                        short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<ParameterMap>>() {

            @Override
            public List<ParameterMap> execute() {
                requireConfigured();
                final Parameters p = admin.getCurrentParameters();
                final ArrayList<ParameterMap> list =
                    new ArrayList<ParameterMap>();
                for (AdminId id : p.getAdminIds()) {
                    list.add(p.get(id).getMap());
                }
                return list;
            }
        });
    }

    @Override
    @SecureR2Method
    public Plan getPlanById(final int planId, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Plan getPlanById(final int planId,
                            AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Plan>() {

            @Override
            public Plan execute() {
                requireConfigured();
                return admin.getPlanById(planId);
            }
        });
    }

    @Override
    @SecureR2Method
    @Deprecated
    public Map<Integer, Plan> getPlans(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Map<Integer, Plan> getPlans(AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Map<Integer, Plan>>() {

            @Override
            public Map<Integer, Plan> execute() {
                requireConfigured();
                return admin.getRecentPlansCopy();
            }
        });
    }

    @Override
    @SecureR2Method
    public void approvePlan(final int planId, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void approvePlan(final int planId,
                            AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.approvePlan(planId);
            }
        });
    }

    @Override
    @SecureR2Method
    public void executePlan(final int planId, final boolean force,
                            short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void executePlan(final int planId, final boolean force,
                            AuthContext authCtx, short serialVersion) {

         aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.executePlan(planId, force);
            }
        });
    }

    @Override
    @SecureR2Method
    public Plan.State awaitPlan(final int planId, final int timeout,
                                final TimeUnit timeUnit, short serialVersion) {
        throw invalidR2MethodException();
    }

   /**
     * Wait for the plan to finish. If a timeout period is specified, return
     * either when the plan finishes or the timeout occurs.
     * @return the current plan status when the call returns. If the call timed
     * out, the plan may still be running.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Plan.State awaitPlan(final int planId, final int timeout,
                                final TimeUnit timeUnit,
                                AuthContext authCtx, short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Plan.State>() {

            @Override
            public Plan.State execute() {
                requireConfigured();
                return admin.awaitPlan(planId, timeout, timeUnit);
            }
        });
    }

    @Override
    @SecureR2Method
    public void cancelPlan(final int planId, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void cancelPlan(final int planId,
                           AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.cancelPlan(planId);
            }
        });
    }

    @Override
    @SecureR2Method
    public void interruptPlan(final int planId, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void interruptPlan(final int planId,
                              AuthContext authCtx,
                              short serialVersion) {

        aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.interruptPlan(planId);
            }
        });
    }

    @Override
    @SecureR2Method
    public void retryPlan(final int planId, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void retryPlan(final int planId,
                          AuthContext authCtx,
                          short serialVersion) {

        aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                admin.executePlan(planId, false);
            }
        });
    }

    /**
     * @deprecated Unused since R2, since the Admin Console has become
     * read-only.
     */
    @Deprecated
    @Override
    @SecureR2Method
    public void createAndExecuteConfigurationPlan(final String kvsName,
                                                  final String dcName,
                                                  final int repFactor,
                                                  short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * @deprecated Unused since R2, since the Admin Console has become
     * read-only.
     */
    @Deprecated
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void createAndExecuteConfigurationPlan(final String kvsName,
                                                  final String dcName,
                                                  final int repFactor,
                                                  AuthContext authCtx,
                                                  short serialVersion) {

        aservice.getFaultHandler().execute
             (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                final Admin a = admin;
                int planId = a.createDeployDatacenterPlan(
                    "Bootstrap Zone Deployment", dcName, repFactor,
                    DatacenterType.PRIMARY);
                    a.approvePlan(planId);
                    a.executePlan(planId, false);
                    a.awaitPlan(planId, 0, null);
                    a.assertSuccess(planId);

                    final AdminServiceParams asp = aservice.getParams();
                    final StorageNodeParams snp = asp.getStorageNodeParams();
                    planId = a.createDeploySNPlan
                        ("Bootstrap StorageNode Deployment",
                         new DatacenterId(1), snp);
                    a.approvePlan(planId);
                    a.executePlan(planId, false);
                    a.awaitPlan(planId, 0, null);
                    a.assertSuccess(planId);

                    planId = a.createDeployAdminPlan
                        ("Bootstrap Admin Deployment",
                         snp.getStorageNodeId(),
                         asp.getAdminParams().getHttpPort());
                    a.approvePlan(planId);
                    a.executePlan(planId, false);
                    a.awaitPlan(planId, 0, null);
                    a.assertSuccess(planId);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createDeployDatacenterPlan(final String planName,
                                          final String datacenterName,
                                          final int repFactor,
                                          final String datacenterComment,
                                          short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Note that datacenterComment is unused, and is deprecated as of R2.  This
     * method is only used by R2 and earlier clients.
     */
    // TBD:DELETE??
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createDeployDatacenterPlan(final String planName,
                                          final String datacenterName,
                                          final int repFactor,
                                          final String datacenterComment,
                                          AuthContext authCtx,
                                          short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createDeployDatacenterPlan(
                    planName, datacenterName, repFactor,
                    DatacenterType.PRIMARY);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createDeployDatacenterPlan(final String planName,
                                          final String datacenterName,
                                          final int repFactor,
                                          final DatacenterType datacenterType,
                                          final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createDeployDatacenterPlan(final String planName,
                                          final String datacenterName,
                                          final int repFactor,
                                          final DatacenterType datacenterType,
                                          AuthContext authCtx,
                                          final short serialVersion) {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.SimpleOperation<Integer>() {
                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.createDeployDatacenterPlan(
                        planName, datacenterName, repFactor, datacenterType);
                }
            });
    }

    @Override
    @SecureR2Method
    public int createDeploySNPlan(final String planName,
                                  final DatacenterId datacenterId,
                                  final String hostName,
                                  final int registryPort,
                                  final String comment,
                                  short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Creates a plan to deploy a storage node, and stores it by id.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createDeploySNPlan(final String planName,
                                  final DatacenterId datacenterId,
                                  final String hostName,
                                  final int registryPort,
                                  final String comment,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                final ParameterMap pMap = admin.copyPolicy();
                final StorageNodeParams snParams =
                    new StorageNodeParams
                    (pMap, null, hostName, registryPort, comment);
                return admin.createDeploySNPlan(planName, datacenterId,
                                                snParams);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createDeployAdminPlan(final String planName,
                                     final StorageNodeId snid,
                                     final int httpPort,
                                     short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Creates a plan to deploy an Admin, and stores it by its plan id.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createDeployAdminPlan(final String planName,
                                     final StorageNodeId snid,
                                     final int httpPort,
                                     AuthContext authCtx,
                                     short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createDeployAdminPlan(planName, snid,
                                                        httpPort);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createRemoveAdminPlan(final String planName,
                                     final DatacenterId dcid,
                                     final AdminId aid,
                                     short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createRemoveAdminPlan(final String planName,
                                     final DatacenterId dcid,
                                     final AdminId aid,
                                     AuthContext authCtx,
                                     short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createRemoveAdminPlan(planName, dcid, aid);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createDeployTopologyPlan(final String planName,
                                        final String candidateName,
                                        short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createDeployTopologyPlan(final String planName,
                                        final String candidateName,
                                        AuthContext authCtx,
                                        short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createDeployTopoPlan(planName, candidateName);
            }
        });

    }

    @Override
    @SecureR2Method
    public int createStopAllRepNodesPlan(final String planName,
                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Creates a plan to stop all RepNodes in a kvstore.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createStopAllRepNodesPlan(final String planName,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createStopAllRepNodesPlan(planName);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createStartAllRepNodesPlan(final String planName,
                                          short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Creates a plan to start all RepNodes in a kvstore.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createStartAllRepNodesPlan(final String planName,
                                          AuthContext authCtx,
                                          short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createStartAllRepNodesPlan(planName);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createStopRepNodesPlan(final String planName,
                                      final Set<RepNodeId> ids,
                                      short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Creates a plan to stop the given RepNodes.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createStopRepNodesPlan(final String planName,
                                      final Set<RepNodeId> ids,
                                      AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createStopRepNodesPlan(planName, ids);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createStartRepNodesPlan(final String planName,
                                       final Set<RepNodeId> ids,
                                       short serialVersion) {
        throw invalidR2MethodException();
    }

    /**
     * Creates a plan to start the given RepNodes.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createStartRepNodesPlan(final String planName,
                                       final Set<RepNodeId> ids,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createStartRepNodesPlan(planName, ids);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createChangeParamsPlan(final String planName,
                                      final ResourceId rid,
                                      final ParameterMap newParams,
                                      short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createChangeParamsPlan(final String planName,
                                      final ResourceId rid,
                                      final ParameterMap newParams,
                                      AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.
                    createChangeParamsPlan(planName, rid, newParams);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createChangeAllParamsPlan(final String planName,
                                         final DatacenterId dcid,
                                         final ParameterMap newParams,
                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createChangeAllParamsPlan(final String planName,
                                         final DatacenterId dcid,
                                         final ParameterMap newParams,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.
                    createChangeAllParamsPlan(planName, dcid, newParams);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createChangeAllAdminsPlan(final String planName,
                                         final DatacenterId dcid,
                                         final ParameterMap newParams,
                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createChangeAllAdminsPlan(final String planName,
                                         final DatacenterId dcid,
                                         final ParameterMap newParams,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.
                    createChangeAllAdminsPlan(planName, dcid, newParams);
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int
        createChangeGlobalSecurityParamsPlan(final String planName,
                                             final ParameterMap newParams,
                                             AuthContext authCtx,
                                             short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createChangeGlobalSecurityParamsPlan(
                    planName, newParams);
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.ADMIN })
    public int createCreateUserPlan(final String planName,
                                    final String userName,
                                    final boolean isEnabled,
                                    final boolean isAdmin,
                                    final char[] plainPassword,
                                    AuthContext authCtx,
                                    short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.
                        createCreateUserPlan(planName, userName, isEnabled,
                                             isAdmin, plainPassword);
                }
            });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.ADMIN })
    public int createDropUserPlan(final String planName,
                                  final String userName,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.
                        createDropUserPlan(planName, userName);
                }
            });
    }

    /* no R2-compatible version */
    @Override
    /* Additional internal checks if the modified user != this user */
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createChangeUserPlan(final String planName,
                                    final String userName,
                                    final Boolean isEnabled,
                                    final char[] plainPassword,
                                    final boolean retainPassword,
                                    final boolean clearRetainedPassword,
                                    AuthContext authCtx,
                                    short serialVersion) {

        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Integer>() {

                @Override
                public Integer execute() {
                    requireConfigured();
                    return admin.
                        createChangeUserPlan(planName, userName, isEnabled,
                                             plainPassword, retainPassword,
                                             clearRetainedPassword);
                }
            });
    }

    @Override
    @SecureR2Method
    public int createMigrateSNPlan(final String planName,
                                   final StorageNodeId oldNode,
                                   final StorageNodeId newNode,
                                   final int newHttpPort,
                                   short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createMigrateSNPlan(final String planName,
                                   final StorageNodeId oldNode,
                                   final StorageNodeId newNode,
                                   final int newHttpPort,
                                   AuthContext authCtx,
                                   short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createMigrateSNPlan(planName, oldNode,
                                                 newNode, newHttpPort);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createRemoveSNPlan(final String planName,
                                  final StorageNodeId targetNode,
                                  short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createRemoveSNPlan(final String planName,
                                  final StorageNodeId targetNode,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createRemoveSNPlan(planName, targetNode);
            }
        });
    }

    @Override
    @SecureR2Method
    public int createRemoveDatacenterPlan(final String planName,
                                          final DatacenterId targetId,
                                          short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createRemoveDatacenterPlan(final String planName,
                                          final DatacenterId targetId,
                                          AuthContext authCtx,
                                          short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createRemoveDatacenterPlan(planName, targetId);
            }
        });
    }

    /** Create a plan that will address mismatches in the topology */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.ADMIN })
    public int createRepairPlan(final String planName,
                                AuthContext authCtx,
                                short serialVersion) {
      return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createRepairPlan(planName);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createAddTablePlan(final String planName,
                                  final String tableId,
                                  final String tableName,
                                  final FieldMap fieldMap,
                                  final List<String> primaryKey,
                                  final List<String> majorKey,
                                  final boolean r2compat,
                                  final int schemaId,
                                  final String description,
                                  AuthContext authCtx,
                                  short serialVersion) {

       return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                if (schemaId != 0) {
                    validateSchemaId(schemaId);
                }
                return admin.createAddTablePlan(planName, tableId,
                                                tableName, fieldMap,
                                                primaryKey,
                                                majorKey, r2compat,
                                                schemaId, description);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createRemoveTablePlan(final String planName,
                                     final String tableName,
                                     final boolean removeData,
                                     AuthContext authCtx,
                                     short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createRemoveTablePlan(planName,
                                                   tableName,
                                                   removeData);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createAddIndexPlan(final String planName,
                                  final String indexName,
                                  final String tableName,
                                  final String[] indexedFields,
                                  final String description,
                                  AuthContext authCtx,
                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createAddIndexPlan(planName, indexName,
                                                tableName,
                                                indexedFields, description);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createRemoveIndexPlan(final String planName,
                                     final String indexName,
                                     final String tableName,
                                     AuthContext authCtx,
                                     short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createRemoveIndexPlan(planName, indexName,
                                                   tableName);
            }
        });
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int createEvolveTablePlan(final String planName,
                                     final String tableName,
                                     final int tableVersion,
                                     final FieldMap fieldMap,
                                     AuthContext authCtx,
                                     short serialVersion) {

       return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Integer>() {

            @Override
            public Integer execute() {
                requireConfigured();
                return admin.createEvolveTablePlan(planName,
                                                   tableName,
                                                   tableVersion,
                                                   fieldMap);
            }
        });
    }

    /**
     * This is only used by table code, maybe change it to be table-specific.
     */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public <T extends Metadata<? extends MetadataInfo>> T
                                 getMetadata(final Class<T> returnType,
                                             final MetadataType metadataType,
                                             AuthContext authCtx,
                                             short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<T>() {

                @Override
                public  T execute() {
                    requireConfigured();
                    return admin.getMetadata(returnType, metadataType);
                }
            });
    }

    @Override
    @SecureR2Method
    public Topology getTopology(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Topology getTopology(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Topology>() {

            @Override
            public Topology execute() {
                requireConfigured();
                return admin.getCurrentTopology();
            }
        });
    }

    @Override
    @SecureR2Method
    public Parameters getParameters(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Parameters getParameters(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Parameters>() {

            @Override
            public Parameters execute() {
                requireConfigured();
                return admin.getCurrentParameters();
            }
        });
    }

    @Override
    @SecureR2Method
    public ParameterMap getRepNodeParameters(final RepNodeId rnid,
                                             short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public ParameterMap getRepNodeParameters(final RepNodeId rnid,
                                             AuthContext authCtx,
                                             short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<ParameterMap>() {

            @Override
            public ParameterMap execute() {
                requireConfigured();
                final Parameters p = admin.getCurrentParameters();
                if (p.get(rnid) != null) {
                    return p.get(rnid).getMap();
                }
                throw new IllegalCommandException
                        ("RepNode does not exist: " + rnid);
            }
        });
    }

    @Override
    @SecureR2Method
    public ParameterMap getPolicyParameters(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public ParameterMap getPolicyParameters(AuthContext authCtx,
                                            short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<ParameterMap>() {

            @Override
            public ParameterMap execute() {
                requireConfigured();
                final Parameters p = admin.getCurrentParameters();
                return p.getPolicies();
            }
        });
    }

    @Override
    @SecureR2Method
    public void newParameters(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void newParameters(AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {

                /**
                 * Allow this call even if not configured.  This call can be
                 * made as a side effect of a storage node being registered and
                 * especially in test environments it can be difficult to know
                 * the configured state.
                 */
                if (admin != null) {
                    admin.newParameters();
                }
            }
        });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void newGlobalParameters(AuthContext authCtx, short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {

                /**
                 * Allow this call even if not configured.  This call can be
                 * made as a side effect of a storage node being registered and
                 * especially in test environments it can be difficult to know
                 * the configured state.
                 */
                if (admin != null) {
                    admin.newGlobalParameters();
                }
            }
        });
    }

    @Override
    @SecureR2Method
    public void configure(final String storeName, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void configure(final String storeName,
                          AuthContext authCtx,
                          short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireNotConfigured();
                aservice.configure(storeName);
                /* Admin should be non-null now */
                admin = aservice.getAdmin();
            }
        });
    }

    @Override
    @SecureR2Method
    public String getStoreName(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String getStoreName(AuthContext authCtx, short serialVersion) {
        if (admin != null) {
            return admin.getParams().getGlobalParams().getKVStoreName();
        }
        return null;
    }

    @Override
    @SecureR2Method
    public String getRootDir(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String getRootDir(AuthContext authCtx, short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

             @Override
             public String execute() {
                 if (admin != null) {
                     return admin.getParams().getStorageNodeParams().
                         getRootDirPath();
                 }
                 return null;
             }
        });
    }

    @Override
    @SecureR2Method
    public void stop(final boolean force, short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void stop(final boolean force,
                     AuthContext authCtx,
                     short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                aservice.stop(force);
                try {
                    if (rti != null) {
                        rti.stop(SerialVersion.CURRENT);
                    }
                } catch (RemoteException e) {
                    if (admin != null) {
                        final Logger logger = admin.getLogger();

                        /*
                         * Logger may not be available, as the admin may not be
                         * created yet.
                         */
                        if (logger != null) {
                            logger.log(Level.INFO,
                                       "Ignore exception while stopping " +
                                       " remoteTestInterface", e);
                        }
                    }
                }
                return;
            }
        });
    }

    @Override
    @SecureR2Method
    public void setPolicies(final ParameterMap policyMap,
                            short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void setPolicies(final ParameterMap policyMap,
                            AuthContext authCtx,
                            short serialVersion) {
        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                admin.setPolicy(policyMap);
            }
        });
    }

    @Override
    @SecureR2Method
    public Map<ResourceId, ServiceChange> getStatusMap(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Map<ResourceId, ServiceChange> getStatusMap(AuthContext authCtx,
                                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation
             <Map<ResourceId, ServiceChange>>() {

            @Override
            public Map<ResourceId, ServiceChange> execute() {
                requireConfigured();
                return admin.getMonitor().getServiceChangeTracker().getStatus();
            }
        });
    }

    @Override
    @SecureR2Method
    public Map<ResourceId, PerfEvent> getPerfMap(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Map<ResourceId, PerfEvent> getPerfMap(AuthContext authCtx,
                                                 short serialVersion) {

        return aservice.getFaultHandler().
        execute(new ProcessFaultHandler.SimpleOperation
                <Map<ResourceId, PerfEvent>>() {

            @Override
            public Map<ResourceId, PerfEvent> execute() {
                requireConfigured();
                return admin.getMonitor().getPerfTracker().getPerf();
            }
        });
    }

    @Override
    @SecureR2Method
    public RetrievedEvents<ServiceChange> getStatusSince(final long since,
                                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public RetrievedEvents<ServiceChange> getStatusSince(final long since,
                                                         AuthContext authCtx,
                                                         short serialVersion) {

        return aservice.getFaultHandler().
        execute(new ProcessFaultHandler.SimpleOperation
                <RetrievedEvents<ServiceChange>>() {

            @Override
            public RetrievedEvents<ServiceChange> execute() {
                requireConfigured();
                return admin.getMonitor().getServiceChangeTracker().
                    retrieveNewEvents(since);
            }
        });
    }

    @Override
    @SecureR2Method
    public RetrievedEvents<PerfEvent> getPerfSince(long since,
                                                   short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public RetrievedEvents<PerfEvent> getPerfSince(long since,
                                                   AuthContext authCtx,
                                                   short serialVersion) {

        requireConfigured();
        return admin.getMonitor().getPerfTracker().retrieveNewEvents(since);
    }

    @Override
    @SecureR2Method
    public RetrievedEvents<LogRecord> getLogSince(long since,
                                                  short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public RetrievedEvents<LogRecord> getLogSince(long since,
                                                  AuthContext authCtx,
                                                  short serialVersion) {

        requireConfigured();
        return admin.getMonitor().getLogTracker().retrieveNewEvents(since);
    }

    @Override
    @SecureR2Method
    public RetrievedEvents<PlanStateChange> getPlanSince(long since,
                                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public RetrievedEvents<PlanStateChange> getPlanSince(long since,
                                                         AuthContext authCtx,
                                                         short serialVersion) {

        requireConfigured();
        return admin.getMonitor().getPlanTracker().retrieveNewEvents(since);
    }

    @Override
    @SecureR2Method
    public void registerLogTrackerListener(TrackerListener tl,
                                           short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void registerLogTrackerListener(TrackerListener tl,
                                           AuthContext authCtx,
                                           short serialVersion) {

        requireConfigured();
        admin.getMonitor().getLogTracker().registerListener(tl);
    }

    @Override
    @SecureR2Method
    public void removeLogTrackerListener(TrackerListener tl,
                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void removeLogTrackerListener(TrackerListener tl,
                                         AuthContext authCtx,
                                         short serialVersion) {

        requireConfigured();
        admin.getMonitor().getLogTracker().removeListener(tl);
    }

    @Override
    @SecureR2Method
    public void registerStatusTrackerListener(TrackerListener tl,
                                              short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void registerStatusTrackerListener(TrackerListener tl,
                                              AuthContext authCtx,
                                              short serialVersion) {

        requireConfigured();
        admin.getMonitor().getServiceChangeTracker().registerListener(tl);
    }

    @Override
    @SecureR2Method
    public void removeStatusTrackerListener(TrackerListener tl,
                                            short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void removeStatusTrackerListener(TrackerListener tl,
                                            AuthContext authCtx,
                                            short serialVersion) {

        requireConfigured();
        admin.getMonitor().getServiceChangeTracker().removeListener(tl);
    }

    @Override
    @SecureR2Method
    public void registerPerfTrackerListener(TrackerListener tl,
                                            short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void registerPerfTrackerListener(TrackerListener tl,
                                            AuthContext authCtx,
                                            short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPerfTracker().registerListener(tl);
    }

    @Override
    @SecureR2Method
    public void removePerfTrackerListener(TrackerListener tl,
                                          short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void removePerfTrackerListener(TrackerListener tl,
                                          AuthContext authCtx,
                                          short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPerfTracker().removeListener(tl);
    }

    @Override
    @SecureR2Method
    public void registerPlanTrackerListener(TrackerListener tl,
                                            short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void registerPlanTrackerListener(TrackerListener tl,
                                            AuthContext authCtx,
                                            short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPlanTracker().registerListener(tl);
    }

    @Override
    @SecureR2Method
    public void removePlanTrackerListener(TrackerListener tl,
                                          short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void removePlanTrackerListener(TrackerListener tl,
                                          AuthContext authCtx,
                                          short serialVersion) {

        requireConfigured();
        admin.getMonitor().getPlanTracker().removeListener(tl);
    }

    @Override
    @SecureR2Method
    public Map<String, Long> getLogFileNames(final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Map<String, Long> getLogFileNames(AuthContext authCtx,
                                             final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Map<String, Long>>() {

             @Override
             public Map<String, Long> execute() {

                 requireConfigured();

                 final Map<String, Long> namesToTimes =
                     new HashMap<String, Long>();

                 final File logDir = FileNames.getLoggingDir
                    (new File(getRootDir(serialVersion)),
                     getStoreName(serialVersion));

                 final File[] files = logDir.listFiles(new FilenameFilter() {
                     @Override
                     public boolean accept(File dir, String fname) {
                         return fname.endsWith(FileNames.LOG_FILE_SUFFIX) ||
                             fname.endsWith(FileNames.PERF_FILE_SUFFIX) ||
                             fname.endsWith(FileNames.STAT_FILE_SUFFIX) ||
                             fname.endsWith(FileNames.DETAIL_CSV) ||
                             fname.endsWith(FileNames.SUMMARY_CSV);
                     }
                 });

                 for (File f : files) {
                     final String name = f.getName();
                     final long modTime = f.lastModified();

                     namesToTimes.put(name, new Long(modTime));
                 }
                 return namesToTimes;
             }
         });
    }

    private void checkReady() {
        if (!admin.isReady()) {
            throw new IllegalCommandException
                ("The Admin is not ready to serve this request.  " +
                 "Please try again later");
        }
    }

    private void requireConfigured() {
        if (admin == null) {
            throw new IllegalCommandException
                ("This command can't be used until the Admin is configured.");
        }
        checkReady();
    }

    private void requireNotConfigured() {
        if (admin != null) {
            throw new IllegalCommandException
                ("This command is valid only in bootstrap/configuration mode.");
        }
    }

    @Override
    @SecureR2Method
    public ReplicatedEnvironment.State getAdminState
        (final short serialVersion) {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public ReplicatedEnvironment.State getAdminState
        (AuthContext authCtx, final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.
             SimpleOperation<ReplicatedEnvironment.State>() {

            @Override
            public ReplicatedEnvironment.State execute() {

                /*
                 * Don't talk to pre-V2 versions of the client.
                 *
                 * Note: This serial version check counts on getAdminState()
                 * always being the first RMI call from the client.
                 */
                if (serialVersion < SerialVersion.V2) {
                    throw new UnsupportedOperationException
                        ("The client is incompatible with the Admin service. " +
                         "Please upgrade the client to version " +
                         KVVersion.CURRENT_VERSION.getNumericVersionString());
                }

                if (admin == null) {
                    return null; /* indicates unconfigured */
                }

                return admin.getReplicationMode();
            }
        });
    }

    @Override
    @SecureR2Method
    public URI getMasterRmiAddress(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public URI getMasterRmiAddress(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<URI>() {

            @Override
            public URI execute() {
                requireConfigured();
                return admin.getMasterRmiAddress();
            }
        });
    }

    @Override
    @SecureR2Method
    public URI getMasterHttpAddress(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public URI getMasterHttpAddress(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<URI>() {

            @Override
            public URI execute() {
                requireConfigured();
                return admin.getMasterHttpAddress();
            }
        });
    }

    @Override
    @SecureR2Method
    public List<CriticalEvent> getEvents(final long startTime,
                                         final long endTime,
                                         final CriticalEvent.EventType type,
                                         short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public List<CriticalEvent> getEvents(final long startTime,
                                         final long endTime,
                                         final CriticalEvent.EventType type,
                                         AuthContext authCtx,
                                         short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<CriticalEvent>>() {

            @Override
            public List<CriticalEvent> execute() {
                requireConfigured();
                return admin.getEvents(startTime, endTime, type);
            }
        });
    }

    @Override
    @SecureR2Method
    public CriticalEvent getOneEvent(final String eventId,
                                     short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public CriticalEvent getOneEvent(final String eventId,
                                     AuthContext authCtx,
                                     short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<CriticalEvent>() {

            @Override
            public CriticalEvent execute() {
                requireConfigured();
                return admin.getOneEvent(eventId);
            }
        });
    }

    @Override
    @SecureR2Method
    public String [] startBackup(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String [] startBackup(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String []>() {
            @Override
            public String [] execute() {
                requireConfigured();
                final ReplicatedEnvironment env = admin.getEnv();
                if (dbBackup != null) {
                    dbBackup.endBackup();
                }
                dbBackup = new DbBackup(env);
                dbBackup.startBackup();
                return dbBackup.getLogFilesInBackupSet();
            }
        });
    }

    @Override
    @SecureR2Method
    public long stopBackup(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public long stopBackup(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Long>() {

            @Override
            public Long execute() {
                requireConfigured();
                long lastFile = -1;
                if (dbBackup != null) {
                    lastFile = dbBackup.getLastFileInBackupSet();
                    dbBackup.endBackup();
                    dbBackup = null;
                }
                return lastFile;
            }
        });
    }

    @Override
    @SecureR2Method
    public void updateMemberHAAddress(final AdminId targetId,
                                      final String targetHelperHosts,
                                      final String newNodeHostPort,
                                      short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void updateMemberHAAddress(final AdminId targetId,
                                      final String targetHelperHosts,
                                      final String newNodeHostPort,
                                      AuthContext authCtx,
                                      short serialVersion) {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {

            @Override
            public void execute() {
                requireConfigured();
                aservice.updateMemberHAAddress(targetId,
                                               targetHelperHosts,
                                               newNodeHostPort);
            }
        });
    }

    @Override
    @SecureR2Method
    public VerifyResults verifyConfiguration(final boolean showProgress,
                                             final boolean listAll,
                                             short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public VerifyResults verifyConfiguration(final boolean showProgress,
                                             final boolean listAll,
                                             AuthContext authCtx,
                                             short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerifyResults>() {

            @Override
            public VerifyResults execute() {

                requireConfigured();
                final VerifyConfiguration checker =
                    new VerifyConfiguration(admin,
                                            showProgress,
                                            listAll,
                                            admin.getLogger());
                checker.verifyTopology();
                return checker.getResults();
            }
        });
    }

    @Override
    @SecureR2Method
    public VerifyResults verifyUpgrade(final KVVersion targetVersion,
                                       final List<StorageNodeId> snIds,
                                       final boolean showProgress,
                                       final boolean listAll,
                                       short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public VerifyResults verifyUpgrade(final KVVersion targetVersion,
                                       final List<StorageNodeId> snIds,
                                       final boolean showProgress,
                                       final boolean listAll,
                                       AuthContext authCtx,
                                       short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerifyResults>() {

            @Override
            public VerifyResults execute() {

                requireConfigured();
                final VerifyConfiguration checker =
                    new VerifyConfiguration(admin,
                                            showProgress,
                                            listAll,
                                            admin.getLogger());
                checker.verifyUpgrade(targetVersion, snIds);
                return checker.getResults();
            }
        });
    }

    @Override
    @SecureR2Method
    public VerifyResults verifyPrerequisite(final KVVersion targetVersion,
                                            final KVVersion prerequisiteVersion,
                                            final List<StorageNodeId> snIds,
                                            final boolean showProgress,
                                            final boolean listAll,
                                            final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public VerifyResults verifyPrerequisite(final KVVersion targetVersion,
                                            final KVVersion prerequisiteVersion,
                                            final List<StorageNodeId> snIds,
                                            final boolean showProgress,
                                            final boolean listAll,
                                            AuthContext authCtx,
                                            final short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<VerifyResults>() {

            @Override
            public VerifyResults execute() {

                requireConfigured();
                final VerifyConfiguration checker =
                    new VerifyConfiguration(admin,
                                            showProgress,
                                            listAll,
                                            admin.getLogger());
                checker.verifyPrerequisite(targetVersion,
                                           prerequisiteVersion,
                                           snIds);
                return checker.getResults();
            }
        });
    }

    @Override
    @SecureR2Method
    public LoadParameters getParams(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public LoadParameters getParams(AuthContext authCtx, short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<LoadParameters>() {

            @Override
            public LoadParameters execute() {
                requireConfigured();
                return admin.getAllParams();
            }
        });
    }

    @Override
    @SecureR2Method
    public String getStorewideLogName(short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String getStorewideLogName(AuthContext authCtx,
                                      short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return admin.getStorewideLogName();
            }
        });
    }

    @Override
    @SecureR2Method
    public TopologyCandidate getTopologyCandidate(final String candidateName,
                                                  short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public TopologyCandidate getTopologyCandidate(final String candidateName,
                                                  AuthContext authCtx,
                                                  short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<TopologyCandidate>() {

           @Override
           public TopologyCandidate execute() {
               requireConfigured();
               return admin.getCandidate(candidateName);
           }
        });
    }

    @Override
    @SecureR2Method
    public List<String> getTopologyHistory(final boolean concise,
                                           short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public List<String> getTopologyHistory(final boolean concise,
                                           AuthContext authCtx,
                                           short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<List<String>>() {

           @Override
           public List<String> execute() {
               requireConfigured();
               return admin.displayRealizedTopologies(concise);
           }
        });
    }

    @Override
    @SecureR2Method
    public SortedMap<String, AvroDdl.SchemaSummary>
        getSchemaSummaries(final boolean includeDisabled,
                           final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public SortedMap<String, AvroDdl.SchemaSummary>
        getSchemaSummaries(final boolean includeDisabled,
                           AuthContext authCtx,
                           final short serialVersion) {

        requireConfigured();

        return AvroDdl.execute
            (aservice,
             new AvroDdl.Command<SortedMap<String, AvroDdl.SchemaSummary>>() {

            @Override
            public SortedMap<String, AvroDdl.SchemaSummary>
                execute(AvroDdl ddl) {
                return ddl.getSchemaSummaries(includeDisabled);
            }
        });
    }

    @Override
    @SecureR2Method
    public AvroDdl.SchemaDetails getSchemaDetails(final int schemaId,
                                                  final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public AvroDdl.SchemaDetails getSchemaDetails(final int schemaId,
                                                  AuthContext authCtx,
                                                  final short serialVersion) {
        requireConfigured();

        return AvroDdl.execute(aservice,
                               new AvroDdl.Command<AvroDdl.SchemaDetails>() {
            @Override
            public AvroDdl.SchemaDetails execute(AvroDdl ddl) {
                return ddl.getSchemaDetails(schemaId);
            }
        });
    }

    @Override
    @SecureR2Method
    public AvroDdl.AddSchemaResult
        addSchema(final AvroSchemaMetadata metadata,
                  final String schemaText,
                  final AvroDdl.AddSchemaOptions options,
                  final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public AvroDdl.AddSchemaResult
        addSchema(final AvroSchemaMetadata metadata,
                  final String schemaText,
                  final AvroDdl.AddSchemaOptions options,
                  final AuthContext authCtx,
                  final short serialVersion) {

        requireConfigured();

        return AvroDdl.execute(aservice,
                               new AvroDdl.Command<AvroDdl.AddSchemaResult>() {

            @Override
            public AvroDdl.AddSchemaResult execute(AvroDdl ddl) {
                return ddl.addSchema(metadata,
                                     schemaText,
                                     options,
                                     admin.getStoreVersion());
            }
        });
    }

    @Override
    @SecureR2Method
    public boolean updateSchemaStatus(final int schemaId,
                                      final AvroSchemaMetadata newMeta,
                                      final short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public boolean updateSchemaStatus(final int schemaId,
                                      final AvroSchemaMetadata newMeta,
                                      final AuthContext authCtx,
                                      final short serialVersion) {

        requireConfigured();

        return AvroDdl.execute(aservice, new AvroDdl.Command<Boolean>() {

            @Override
            public Boolean execute(AvroDdl ddl) {
                return ddl.updateSchemaStatus(schemaId,
                                              newMeta,
                                              admin.getStoreVersion());
            }
        });
    }

    @Override
    @SecureR2Method
    public void assertSuccess(final int planId, short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void assertSuccess(final int planId,
                              final AuthContext authCtx,
                              short serialVersion)
        throws RemoteException {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
            @Override
             public void execute() {
                requireConfigured();
                admin.assertSuccess(planId);
            }
        });
    }

    @Override
    @SecureR2Method
    public String getPlanStatus(final int planId,
                                final long options,
                                short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String getPlanStatus(final int planId,
                                final long options,
                                final AuthContext authCtx,
                                short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();

                return admin.getPlanStatus(planId, options);
            }
        });
    }

    @Override
    @SecureR2Method
    public void installStatusReceiver(final AdminStatusReceiver asr,
                                      short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public void installStatusReceiver(final AdminStatusReceiver asr,
                                      final AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {

        aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleProcedure() {
            @Override
            public void execute() {
                aservice.installStatusReceiver(asr);
            }
        });
    }

    @Override
    @SecureR2Method
    public String getUpgradeOrder(final KVVersion targetVersion,
                                  final KVVersion prerequisiteVersion,
                                  short serialVersion)
        throws RemoteException {

        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public String getUpgradeOrder(final KVVersion targetVersion,
                                  final KVVersion prerequisiteVersion,
                                  final AuthContext authCtx,
                                  short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<String>() {

            @Override
            public String execute() {
                requireConfigured();
                return UpgradeUtil.generateUpgradeList(admin,
                                                       targetVersion,
                                                       prerequisiteVersion);
            }
        });
    }

    @Override
    @SecureR2Method
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany,
                                short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public int[] getPlanIdRange(final long startTime,
                                final long endTime,
                                final int howMany,
                                AuthContext authCtx,
                                short serialVersion) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<int[]>() {

            @Override
            public int[] execute() {
                requireConfigured();

                return admin.getPlanIdRange(startTime, endTime, howMany);
            }
        });
    }

    @Override
    @SecureR2Method
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany,
                                           short serialVersion) {
        throw invalidR2MethodException();
    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Map<Integer, Plan> getPlanRange(final int firstPlanId,
                                           final int howMany,
                                           AuthContext authCtx,
                                           short serialVersion) {
        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<Map<Integer, Plan>>() {

            @Override
            public Map<Integer, Plan> execute() {
                requireConfigured();

                return admin.getPlanRange(firstPlanId, howMany);
            }
        });

    }

    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public Map<String, UserDescription>
        getUsersDescription(final AuthContext authCtx, short serialVersion)
        throws RemoteException {

        return aservice.getFaultHandler().execute(
            new ProcessFaultHandler.
                SimpleOperation<Map<String, UserDescription>>() {

                @Override
                public Map<String, UserDescription> execute() {
                    requireConfigured();

                    final SecurityMetadata md =
                            admin.getMetadata(SecurityMetadata.class,
                                              MetadataType.SECURITY);
                    return md == null ?
                           null : md.getUsersDescription();
                }
            });
    }

    /* no R2-compatible version */
    @Override
    @SecureAutoMethod(roles = { KVStoreRole.AUTHENTICATED })
    public boolean verifyUserPassword(final String userName,
                                      final char[] password,
                                      AuthContext authCtx,
                                      short serialVersion)
        throws RemoteException {
        return aservice.getFaultHandler().execute
                (new ProcessFaultHandler.SimpleOperation<Boolean>() {

                @Override
                public Boolean execute() {
                    requireConfigured();

                    final SecurityMetadata md =
                            admin.getMetadata(SecurityMetadata.class,
                                              MetadataType.SECURITY);
                    return md == null ?
                           false : md.verifyUserPassword(userName, password);
                }
            });
    }

    /**
     * Make sure that the schema id passed in for a table creation is a valid
     * schema id in the system.  If not found this call with throw
     * IllegalCommandException.
     */
    @SuppressWarnings("unused")
    private void  validateSchemaId(final int schemaId) {
        final AvroDdl.SchemaDetails details =
            getSchemaDetails(schemaId, (AuthContext) null, (short) 0);
    }
}
