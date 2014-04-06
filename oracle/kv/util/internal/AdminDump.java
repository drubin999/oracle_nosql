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

package oracle.kv.util.internal;

import java.io.File;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.Admin.Memo;
import oracle.kv.impl.admin.criticalevent.CriticalEvent;
import oracle.kv.impl.admin.criticalevent.CriticalEvent.EventKey;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.ParametersHolder;
import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.admin.topo.RealizedTopology;
import oracle.kv.impl.admin.topo.TopologyCandidate;
import oracle.kv.impl.admin.topo.TopologyStore;
import oracle.kv.impl.metadata.MetadataHolder;
import oracle.kv.impl.util.FormatUtils;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.IndexNotAvailableException;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * AdminDump is an unadvertised, in-progress utility which can read and display
 * the contents of the Admin database for debugging and diagnostic purposes.It
 * must be invoked on the node where the Admin environment is hosted.
 * <pre>
 *   AdminDump -h &lt;directory where Admin database is located&gt;
 *             ... flags ... (see usage)
 * </pre>
 * Note that the Admin database is typically in
 * kvroot/store/snX/adminX/env. The je.jar must be in the classpath, as well as
 * the usual kvstore classes.
 *
 * Currently, the utility can 
 * - display RN params
 * - the most recent numTopos deployed topologies are displayed. 
 * - count all the records in the AdminDB
 * - dump, using a cursor, the stored events.
 * It would be a good idea to expand this to display params for SNs and Admins,
 * as well as other future metadata. Also, there should be a refactoring so 
 * that the internal AdminDBCatalog class becomes part of the Admin service. 
 */
public class AdminDump {
    private static int DEFAULT_TOPO_VERSIONS = 0;
    private final PrintStream out;
    private File envHome = null;
    private int showTopos = DEFAULT_TOPO_VERSIONS;
    private boolean showParams = false;
    private boolean showCounts = false;
    private boolean showEvents = false;
    private boolean showModel = false;

    private AdminDump(PrintStream out) {
        this.out = out;
    }

    public static void main(String[] args) throws Exception {
        AdminDump dumper = new AdminDump(System.out);
        dumper.parseArgs(args);
        try {
            dumper.run();
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public void run() {
        out.println("For internal use only.");
        /* 
         * Be sure to display which Admin this is, and what time the dump
         * is taken, in case the information is gathered by a user and 
         * sent to us.
         */
        out.println("Information from admin database in " + envHome);
        DateFormat fm = FormatUtils.getDateTimeAndTimeZoneFormatter();
        out.println("Current time: " +
                     fm.format(new Date(System.currentTimeMillis())));
        
        displayAdminInfo();
    }

    /**
     * Display selected pieces of the admin database
     */
    private void displayAdminInfo() {

        /*
         * Initialize an environment configuration, and create an environment.
         */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        envConfig.setAllowCreate(false);
        Environment env = new Environment(envHome, envConfig);

        AdminDBCatalog catalog = new AdminDBCatalog(env, out);
        catalog.openReadOnlyStore();

        EntityStore estore = catalog.getEntityStore();

        /* Show the types of classes in the adminDB */
        if (showModel) {
            listModel(estore, env);
        }

        /* Count the records in the DB */
        if (showCounts) {
           catalog.countRecords();
        }

        /* Show the events */
        if (showEvents) {
            catalog.displayEvents();
        }

        /*
         * Show all RN params. In the future:
         *  - select RNS to show
         *  - show admin or sn params
         */
        if (showParams) {
           Parameters params = Parameters.fetch(estore, null);
           out.println(params.printRepNodeParams());
        }

        /* Show topologies */
        if (showTopos > 0) {
            List<RealizedTopology> topos =
                TopologyStore.readLastTopos(estore, showTopos);
            for (RealizedTopology t : topos) {
            out.println("--------- deployed topology --------");
            out.println(t.display(false));
            }
        }
    }


    /** 
     * List all class types in the admin db 
     */
    private void listModel(EntityStore estore, Environment env) {
        out.println("-----------------Class types in Admin DB");
        for (String className : estore.getModel().getKnownClasses()) {
            out.println(className);
        }
        out.println("-----------------Stores in Admin DB");
        for (String storeName : EntityStore.getStoreNames(env)){
            out.println(storeName);
        }
    }

    /**
     * Parse the command line parameters.
     *
     * @param argv Input command line parameters.
     */
    public void parseArgs(String argv[]) {

        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            printUsage(null);
            System.exit(0);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = new File(argv[argc++]);
                } else {
                    printUsage("-h requires an argument");
                }
            } else if (thisArg.equals("-numTopos")) {
                showTopos = Integer.parseInt(argv[argc++]);
            } else if (thisArg.equals("-showCounts")) {
                showCounts = true;
            } else if (thisArg.equals("-showParams")) {
                showParams = true;
            } else if (thisArg.equals("-showEvents")) {
                showEvents = true;
            } else if (thisArg.equals("-showModel")) {
                showModel = true;
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
        }

        if (envHome == null) {
            printUsage("-h is a required argument");
        }
    }

    /**
     * Print the usage of this utility.
     *
     * @param message
     */
    private void printUsage(String msg) {
        if (msg != null) {
            out.println(msg);
        }

        out.println("Usage: " + AdminDump.class.getName());
        out.println(" -h <dir>        # admin service environment directory");
        out.println(" -numTopos <num> # number of historical topo versions");
        out.println("                 # to display");
        out.println(" -showParams     # dump RN params");
        out.println(" -showCounts     # show record counts by record type");
        out.println(" -showEvents     # show all events");
        out.println(" -showModel      # show all classes in the db");
        System.exit(-1);
    }

    /**
     * TODO: move this into its own file, and use this to centralize
     * management of the AdminDB.
     */
    private static class AdminDBCatalog {

        private final Environment env;
        private EntityStore estore;
        private PrintStream out;
        
        /* Types of records held in the admin db */
        EntityType[] recordTypes = new EntityType[] {
            new EntityType("events", EventKey.class, CriticalEvent.class),
            new EntityType("metadata", String.class, MetadataHolder.class),
            new EntityType("plans", Integer.class, AbstractPlan.class),
            new EntityType("params", String.class, ParametersHolder.class),
            new EntityType("memo", String.class, Memo.class),
            new EntityType("topoHistory", Long.class, RealizedTopology.class),
            new EntityType("topoCandidates", String.class, 
                           TopologyCandidate.class)
        };
        
        /* Currently, this class can only open the AdminDB in read only mode.*/
        AdminDBCatalog(Environment env, PrintStream out) {
            this.env = env;
            this.out = out;

            /* TODO: supply way to open in r/w mode */
            openReadOnlyStore();
        }

        EntityStore getEntityStore() {
            return estore;
        }

        private void openReadOnlyStore() {
            /* For now, read only access */
            final StoreConfig stConfig = new StoreConfig();
            stConfig.setAllowCreate(false);
            stConfig.setReadOnly(true);
            stConfig.setTransactional(false);

            estore = new EntityStore(env, Admin.ADMIN_STORE_NAME, stConfig);
        }

        /**
         * Open all the primary indices in the store, and return them
         * in a sorted map, where the record type name is the key.
         */
        Map<String, PrimaryIndex<?,?>> getPrimaryIndices() {
            
            Map<String, PrimaryIndex<?,?>> pIdxList =
                new TreeMap<String, PrimaryIndex<?,?>>();

            for (EntityType t : recordTypes) {
                try {
                    pIdxList.put(t.name, estore.getPrimaryIndex(t.primaryKey,
                                                                t.entity));
                } catch (IndexNotAvailableException ignore) {
                    /* Index hasn't been created yet, skip it. */
                }
            }
            
            return pIdxList;
        }

        /**
         * Get a count of all records in the AdminDB, by type.
         */
        private void countRecords() {
        
            long totalCount = 0;
            long count = 0;
            Map<String, PrimaryIndex<?,?>> indexMap = getPrimaryIndices();
            
            out.println("---------------- Counting records");
            for (Map.Entry<String, PrimaryIndex<?,?>> idx:
                     indexMap.entrySet()) {
                String typeName = idx.getKey();
                PrimaryIndex<?,?> pIdx = idx.getValue();
                count = pIdx.count();
                totalCount += count;
                out.println(typeName + " = " + count);
            }
            out.println("total records = " + totalCount);
        }

        /*
         * Differs from CriticalEvents.fetch() because the latter buffers
         * all records into a list.
         */
        private void displayEvents() {
            final PrimaryIndex<EventKey, CriticalEvent> pi =
                estore.getPrimaryIndex(EventKey.class, CriticalEvent.class);

            EntityCursor<CriticalEvent> eventCursor = pi.entities();
            try {
                int i = 0;
                for (CriticalEvent ev : eventCursor) {
                    out.println(i++ + " " + ev);
                }
            } finally {
                eventCursor.close();
            }
        }
        
        /* a struct */
        private class EntityType {
            final String name;
            final Class<?> primaryKey;
            final Class<?> entity;
            
            public EntityType(String name, 
                              Class<?> primaryKey, 
                              Class<?> entity) {
                this.name = name;
                this.primaryKey = primaryKey;
                this.entity = entity;
            }
        }
    }
}
