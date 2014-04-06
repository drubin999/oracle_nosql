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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.measurement.LatencyInfo;
import oracle.kv.impl.monitor.views.PerfEvent;
import oracle.kv.impl.monitor.views.ServiceChange;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.Partition;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;

/**
 * Prints a Topology for visualization.
 */
public final class TopologyPrinter {

    public enum Filter { STORE, DC, RN, SN, SHARD, STATUS, PERF }
    public static final EnumSet<Filter> all =
        EnumSet.allOf(Filter.class);
    public static final EnumSet<Filter> components =
        EnumSet.of(Filter.STORE, Filter.DC, Filter.RN, Filter.SN, Filter.SHARD);
    /**
     * Private constructor to satisfy CheckStyle and to prevent
     * instantiation of this utility class.
     */
    private TopologyPrinter() {
        throw new AssertionError("Instantiated utility class " +
                                 TopologyPrinter.class);
    }

    /**
     * Dumps the topology to "out".
     * @param params SN capacity and RN mount points are stored in the
     * Parameters class from the Admin DB, and is not available in the topology
     * itself. If params is not null, use it to display that kind of
     * information.
     * @param verbose if true, print partitions for each shard.
     */
    public static void printTopology(Topology t,
                                     PrintStream out,
                                     Parameters params,
                                     EnumSet<Filter> filter,
                                     Map<ResourceId, ServiceChange> statusMap,
                                     Map<ResourceId, PerfEvent> perfMap,
                                     boolean verbose) {
        printTopology(t, out, params, filter, verbose, null, statusMap,
                      perfMap);
    }

    /**
     * Dumps the topology to "out".
     *
     * This method can be called on deployed and not-yet-deployed topologies.
     * The latter are tricky, because the Parameters may not contain a
     * RepNodeParams for new RNs. Some information of interest is only available
     * in the RepNodeParam, and not the topology, such as:
     * - ha ports: calculated at deploy time
     * - mount point mappings: in a special map in the candidate
     *
     * This method has to take care to check whether the params-type information
     * is available yet.
     *
     * @param params SN capacity and RN mount points are stored in the
     * Parameters class from the Admin DB, and is not available in the topology
     * itself. If params is not null, use it to display that kind of
     * information.
     * @param filter is used to selectively print portions of the topology.
     * The default is the entire thing.
     * @param verbose if true, print partitions for each shard.
     */
    private static void printTopology(Topology t,
                                      PrintStream out,
                                      Parameters params,
                                      EnumSet<Filter> filter,
                                      boolean verbose,
                                      Map<RepNodeId, String> mountPointMap,
                                      Map<ResourceId, ServiceChange> statusMap,
                                      Map<ResourceId, PerfEvent> perfMap) {

        int indent = 0;
        final int indentAmount = 2;
        final boolean showStatus = filter.contains(Filter.STATUS) &&
            (statusMap != null);
        final boolean showPerf = filter.contains(Filter.PERF) &&
            (perfMap != null);

        /*
         * Display the store name
         */
        final String storeName = t.getKVStoreName();
        if (filter.contains(Filter.STORE)) {
            out.println("store=" + storeName + "  numPartitions=" +
                        t.getPartitionMap().size() + " sequence=" +
                        t.getSequenceNumber());
            indent += indentAmount;
        }

        /*
         * Display datacenters, sorted by data center ID
         */
        if (filter.contains(Filter.DC)) {
            final List<Datacenter> dcList = t.getSortedDatacenters();
            for (final Datacenter dc : dcList) {
                out.println(makeWhiteSpace(indent) +
                            DatacenterId.DATACENTER_PREFIX + ": " + dc);
            }
            if (dcList.size() > 0) {
                out.println();
            }
        }

        /*
         * Display SNs, sorted by SN id.
         */
        final Map<StorageNodeId, List<RepNodeId>> snMap =
            new TreeMap<StorageNodeId, List<RepNodeId>>();
        for (StorageNodeId snid : t.getStorageNodeMap().getAllIds()) {
            snMap.put(snid, new ArrayList<RepNodeId>());
        }
        final List<RepNode> rnList = t.getSortedRepNodes();
        for (RepNode rn: rnList) {
            final List<RepNodeId> l = snMap.get(rn.getStorageNodeId());
            l.add(rn.getResourceId());
        }

        for (Entry<StorageNodeId, List<RepNodeId>> entry : snMap.entrySet()) {
            final StorageNodeId snId = entry.getKey();
            String capacityInfo = null;
            StorageNodeParams snp = null;
            if (params != null) {
                snp = params.get(snId);
                if (snp != null) {
                    capacityInfo = " capacity=" + snp.getCapacity();
                }
            }
            if (filter.contains(Filter.SN)) {
                out.print(makeWhiteSpace(indent) + "sn=" + t.get(snId));
                if (capacityInfo != null) {
                    out.print(capacityInfo);
                }
                if (showStatus) {
                    out.println(" " + getStatus(statusMap, snId));
                } else {
                    out.println();
                }

                indent += indentAmount;
            }

            /* List all the RNs on a given SN */
            if (filter.contains(Filter.RN)) {
                final List<String> rnsWithMountPoints =
                    new ArrayList<String>();
                for (RepNodeId rnId : entry.getValue()) {
                    String oneRN = "[" + rnId + "]";

                    if (showStatus) {
                        oneRN += " " + getStatus(statusMap, rnId, params);
                    }

                    if (!verbose) {
                        out.println(makeWhiteSpace(indent) + oneRN);
                        if (showPerf) {
                            out.println
                                (makeWhiteSpace((indent + 1) * indentAmount) +
                                 getPerf(perfMap, rnId));
                        }
                        continue;
                    }

                    /*
                     * If this is verbose mode, display the root directory or
                     * mount point, if those are available. Root dir SNs will be
                     * displayed first.
                     *
                     * Mount point information is available in two places,
                     * depending on whether this topology is deployed or not. If
                     * not deployed, it's in the topology candidate assigned
                     * mount point map, because that information is more up to
                     * date for a candidate. If there is no map, then this is
                     * a deployed topo, and we should look in the params.
                     */
                    String mountPoint = null;
                    if (mountPointMap != null) {
                        mountPoint = mountPointMap.get(rnId);
                    } else {
                        if (params != null) {
                            final RepNodeParams rnp = params.get(rnId);
                            if (rnp != null) {
                                mountPoint = rnp.getMountPointString();
                            }
                        }
                    }

                    if (mountPoint == null) {
                        out.print(makeWhiteSpace(indent) + oneRN);
                        if (snp == null) {
                            out.println();
                        } else {
                            out.println("  " + snp.getRootDirPath());
                        }
                        if (showPerf) {
                            out.println
                                (makeWhiteSpace((indent + 1) * indentAmount) +
                                 getPerf(perfMap, rnId));
                        }
                    } else {
                        /*
                         * Make a description of the RN and its mount point
                         * and display it below
                         */
                        rnsWithMountPoints.add(oneRN + "  " + mountPoint);
                        if (showPerf) {
                            rnsWithMountPoints.add
                                (makeWhiteSpace((indent + 1) * indentAmount) +
                                 getPerf(perfMap, rnId));
                        }
                    }
                }
                for (String s : rnsWithMountPoints) {
                    out.println(makeWhiteSpace(indent) + s);
                }
            }
            if (filter.contains(Filter.SN)) {
                indent -= indentAmount;
            }
        }

        /*
         * Display rep groups (shards), sorted by ID
         */
        if (filter.contains(Filter.SHARD)) {
            out.println();
            final Map<RepGroupId, List<PartitionId>> partToRG =
                sortPartitions(t);
            for (Map.Entry<RepGroupId, List<PartitionId>> entry :
                     partToRG.entrySet()) {

                final RepGroupId rgId = entry.getKey();
                final List<PartitionId> partIds = entry.getValue();
                final RepGroup rg = t.get(rgId);
                out.println(makeWhiteSpace(indent) + "shard=" + rg +
                            " num partitions=" + partIds.size());

                if (filter.contains(Filter.RN)) {
                    indent += indentAmount;
                    final List<RepNode> rns =
                        new ArrayList<RepNode>(rg.getRepNodes());
                    Collections.sort(rns);
                    for (RepNode rn: rns) {
                        out.print(makeWhiteSpace(indent) + rn);
                        if (verbose && params != null) {
                            final RepNodeParams rnp =
                                params.get(rn.getResourceId());
                            if (rnp != null) {
                                out.print(" haPort=" + rnp.getJENodeHostPort());
                            }
                        }
                        out.println();
                    }
                }

                if (verbose) {
                    out.println(makeWhiteSpace(indent) + "partitions=" +
                                listPartitions(partIds));
                }
                if (filter.contains(Filter.RN)) {
                    indent -= indentAmount;
                }
            }
        }
    }

    /* Format service status information. */
    private static String getStatus(Map<ResourceId, ServiceChange> statusMap,
                                    ResourceId rId) {
        final ServiceChange change = statusMap.get(rId);
        if (change == null) {
            return "UNREPORTED";
        }


        return change.getStatus().toString();
    }

    private static String getStatus(Map<ResourceId, ServiceChange> statusMap,
                                    RepNodeId rnId,
                                    Parameters params) {
        final String status = getStatus(statusMap, rnId);

        /* RNs may be disabled by a stop-RNs plan */
        if (params != null) {
            final RepNodeParams rnp = params.get(rnId);
            if (rnp != null) {
                if (rnp.isDisabled()) {
                    return "Stopped/" + status;
                }
            }
        }

        return status;
    }

    /* Format performance information. */
    private static String getPerf(Map<ResourceId, PerfEvent> perfMap,
                                    ResourceId rId) {
        final PerfEvent perf = perfMap.get(rId);
        if (perf == null) {
            return "No performance info available";
        }

        final StringBuilder sb = new StringBuilder();
        final LatencyInfo singleCum = perf.getSingleCum();
        final LatencyInfo multiCum = perf.getMultiCum();
        sb.append("   single-op avg latency=").
            append(singleCum.getLatency().getAvg()).append(" ms");

        sb.append("   multi-op avg latency=").
            append(multiCum.getLatency().getAvg()).append(" ms");

        if (perf.needsAlert()) {
            sb.append("[ALERT]");
        }

        return sb.toString();
    }

    /**
     * List the partitions ordered by number, and in range form. For example,
     * gaps would be shown like this:
     * 10-40,45-95,100
     */
    public static String listPartitions(List<PartitionId> partIds) {

        if (partIds.isEmpty()) {
            return "";
        }

        Collections.sort(partIds, new Comparator<PartitionId>() {
                @Override
                public int compare(PartitionId o1, PartitionId o2) {
                   return o1.getPartitionId() - o2.getPartitionId();
                }});

        int first = partIds.get(0).getPartitionId();
        int last = first;
        final StringBuilder sb = new StringBuilder();
        sb.append(first);
        for (PartitionId p : partIds) {
            final int pId = p.getPartitionId();

            if (pId == last) {
                continue;
            }

            if (pId == last + 1) {
                last = pId;
                continue;
            }

            if (last > first) {
                sb.append("-").append(last);
            }

            first = pId;
            last = first;
            sb.append(",").append(first);
        }
        if (last > first) {
            sb.append("-").append(last);
        }
        return sb.toString();
    }

    /**
     * Returns the Topology as a String.
     */
    public static String printTopology(Topology t,
                                       Parameters params,
                                       boolean verbose) {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        printTopology(t, new PrintStream(outStream), params, all, verbose,
                      null, null, null);
        return outStream.toString();
    }

    /**
     * Returns the Topology as a String, for logging messages.
     */
    public static String printTopology(Topology t) {
        return printTopology(t, null, false);
    }

    public static String printTopology(TopologyCandidate tc,
                                       Parameters params,
                                       boolean verbose) {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        printTopology(tc.getTopology(), new PrintStream(outStream), params,
                      all, verbose, tc.getAllMountPoints(), null, null);
        return outStream.toString();
    }


    private static String makeWhiteSpace(int indent) {
        String ret = "";
        for (int i = 0; i < indent; i++) {
            ret += " ";
        }
        return ret;
    }

    /**
     * Return a map of rep groups to partitions ids, so one can tell the
     * number of partitions assigned to each rep group.
     */
    private static
        Map<RepGroupId, List<PartitionId>> sortPartitions(Topology t) {

        final Map<RepGroupId, List<PartitionId>> partToRG =
            new TreeMap<RepGroupId, List<PartitionId>>(new RGComparator());

        for (RepGroup rg: t.getRepGroupMap().getAll()) {
            partToRG.put(rg.getResourceId(), new ArrayList<PartitionId>());
        }

        for (Partition p : t.getPartitionMap().getAll()) {
            final List<PartitionId> pIds = partToRG.get(p.getRepGroupId());
            pIds.add(p.getResourceId());
        }
        return partToRG;
    }

    private static class RGComparator implements Comparator<RepGroupId> {

        @Override
        public int compare(RepGroupId o1, RepGroupId o2) {
           return (o1.getGroupId() - o2.getGroupId());
        }
    }

    /**
     * Displays information about one or all zones in the given
     * <code>Topology</code>. If the <code>id</code> and <code>name</code>
     * parameters are both <code>null</code>, then this method will simply list
     * the ids and names of all of the zones in the store. Otherwise, this
     * method will display information about the zone having the given
     * <code>id</code> or <code>name</code>; including all of the storage nodes
     * deployed to that zone, and whether the zone consists of secondary zones.
     * Note that if the <code>id</code> and <code>name</code> parameters
     * are both non-<code>null</code>, then the value of the <code>id</code>
     * parameter will be used.
     *
     * @param id The id of the zone whose information should be displayed.
     * @param name The name of the zone whose information should be displayed.
     * @param topo The <code>Topology</code> from which to retrieve the
     * information to display.
     * @param out The <code>OutputStream</code> to which the desired
     * information will be written for display.
     * @param params If not <code>null</code>, then each storage node's
     * capacity and datacenter type are retrieved from this object and
     * displayed with that storage node's information.
     */
    public static void printZoneInfo(DatacenterId id,
                                     String name,
                                     Topology topo,
                                     PrintStream out,
                                     Parameters params) {

        int indent = 0;
        final int indentAmount = 2;
        final boolean showAll =
            ((id == null) && (name == null) ? true : false);
        Datacenter showZone = null;

        /*
         * Display zones, sorted by ID
         */
        final List<Datacenter> dcList = topo.getSortedDatacenters();
        for (final Datacenter zone : dcList) {
            if (showAll) {
                out.println(makeWhiteSpace(indent) +
                            DatacenterId.DATACENTER_PREFIX + ": " + zone);
            } else {
                if ((id != null) && id.equals(zone.getResourceId())) {
                    showZone = zone;
                    break;
                } else if ((name != null) && name.equals(zone.getName())) {
                    showZone = zone;
                    break;
                }
            }
        }
        if (showAll) {
            return;
        }

        /* If showZone is null, then the id or name input is unknown */
        if (showZone == null) {
            out.println(makeWhiteSpace(indent) +
                        DatacenterId.DATACENTER_PREFIX +
                        ": unknown id or name");
            return;
        }

        /*
         * For the given zone (id or name), display SNs, sorted by SN id.
         */
        out.println(makeWhiteSpace(indent) +
                    DatacenterId.DATACENTER_PREFIX + ": " + showZone);

        final DatacenterId showZoneId = showZone.getResourceId();
        final List<StorageNode> snList = topo.getSortedStorageNodes();
        String capacityInfo = null;
        StorageNodeParams snp = null;
        indent += indentAmount;
        for (StorageNode sn: snList) {
            if (showZoneId.equals(sn.getDatacenterId())) {
                out.print(makeWhiteSpace(indent) + "[" + sn.getResourceId() +
                    "] " + sn.getHostname() + ":" + sn.getRegistryPort());
                if (params != null) {
                    snp = params.get(sn.getResourceId());
                    if (snp != null) {
                        capacityInfo = " capacity=" + snp.getCapacity();
                    }
                }
                if (capacityInfo != null) {
                    out.print(capacityInfo);
                }
                out.println();
            }
        }
    }
}
