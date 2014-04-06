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

package oracle.kv.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import oracle.kv.PasswordCredentials;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.login.LoginManager;
import oracle.kv.impl.security.util.KVStoreLogin;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.util.shell.Shell;

/**
 * Generate configuration files and directory structure for an existing Storage
 * Node.  The resulting zip archive can be unpacked and used as a replace root
 * directory.
 * Usage:
 *   java -cp <kvstore.jar> oracle.kv.util.GenerateConfig -host <host>
 *       -port <registryPort> -sn <storageNodeId> -target <zipfile>
 *       [-username <user>] [-security <security-file-path>]
 *       [-secdir <overriden security directory>]
 *
 * The resulting zipfile for kvroot "myroot" store "mystore" and -snid 1 will
 * have this structure which can be unzipped to the proper root location and
 * used.
 *  myroot/
 *    config.xml
 *    security.policy
 *    mystore/
 *      security.policy
 *      sn1/
 *        config.xml
 *
 * To start the newly-created storage node after unzip:
 *  java -jar <kvstore.jar> start -root <kvroot>
 *
 * If the actual RepNode and/or Admin directories and data are available and
 * placed in the file hierarchy they will be used as well.  If not, they will
 * be recovered via normal JE network restore.
 *
 * This program uses the information available in the Topology and Admin
 * Parameters to generate the required configuration files.  It constructs
 * the expected hierarchy in a temporary directory, zips the files, and removes
 * the temporary files leaving the result in <zipfile>.zip.
 */

public class GenerateConfig {

    public static final String COMMAND_NAME = "generateconfig";
    public static final String COMMAND_DESC =
        "generates configuration files for the specified storage node";
    public static final String COMMAND_ARGS =
        CommandParser.getHostUsage() + " " + CommandParser.getPortUsage() +
        " -sn <StorageNodeId> " + "-target <zipfile>" + Shell.eolt +
        "[" + CommandParser.getUserUsage() + "] [" +
        CommandParser.getSecurityUsage() +
        "] [-secdir <overriden security directory>]";

    private StorageNodeId snid;
    private Parser parser;
    private CommandServiceAPI cs;
    private Topology t;
    private Parameters p;
    private String target;
    private String userName;
    private String securityFilePath;
    private String secDir;
    private boolean isSecured;
    private PasswordCredentials pwdCreds;

    /*
     * The top-level interface that drives the process
     */
    private void generate() throws Exception {

        /*
         * Initialize the store login routines if secured communcation settings
         * are detected
         */
        initLogin();

        /*
         * Find the Admin, get the Topology and Parameters.  StorageNodeId
         * is already set by the command line parser.
         */
        final String host = parser.getHostname();
        final int port = parser.getRegistryPort();
        final LoginManager loginMgr =
            KVStoreLogin.getAdminLoginMgr(host, port, pwdCreds);
        cs = RegistryUtils.getAdmin(host, port, loginMgr);
        t = cs.getTopology();
        p = cs.getParameters();

        /*
         * LoadParameters is a class that knows how to collect and write
         * ParameterMaps.
         */
        LoadParameters lp = new LoadParameters();

        /*
         * Get the single parameter entries -- Global, StorageNode, Admin
         */
        GlobalParams gp = p.getGlobalParams();
        StorageNodeParams snp = p.get(snid);
        lp.addMap(gp.getMap());
        lp.addMap(snp.getMap());
        if (snp.getMountMap() != null) {
            lp.addMap(snp.getMountMap());
        }
        AdminParams ap = getAdminParams();
        if (ap != null) {
            lp.addMap(ap.getMap());
        }

        /*
         * Add any RepNodes associated with the StorageNode
         */
        List<RepNode> rns = t.getSortedRepNodes();
        for (RepNode rn : rns) {
            if (rn.getStorageNodeId().equals(snid)) {
                RepNodeParams rnp = p.get(rn.getResourceId());
                lp.addMap(rnp.getMap());
            }
        }

        /**
         * Construct the bootstrap config information
         */
        LoadParameters bootLp = generateBootConfig(gp, snp, ap);
        File rootDir = new File(snp.getRootDirPath());

        /**
         * Create the expected directory structure and files in rootDir
         */
        createFiles(rootDir.getName(), gp.getKVStoreName(), lp, bootLp);

        /**
         * Create the zip archive
         */
        createZip(rootDir.getName());

        /**
         * Cleanup
         */
        delete(new File(rootDir.getName()));
    }

    /**
     * Find AdminParams if this SN is hosting an admin
     */
    private AdminParams getAdminParams() {
        Collection<AdminParams> aps = p.getAdminParams();
        for (AdminParams ap : aps) {
            if (ap.getStorageNodeId().equals(snid)) {
                return ap;
            }
        }
        return null;
    }

    /**
     * Generate the bootstrap config parameters
     */
    private LoadParameters generateBootConfig(GlobalParams gp,
                                              StorageNodeParams snp,
                                              AdminParams ap)
        throws Exception {

        LoadParameters lp = new LoadParameters();
        boolean hostingAdmin = false;
        int adminPort = 0;
        if (ap != null) {
            hostingAdmin = true;
            adminPort = ap.getHttpPort();
        }

        BootstrapParams bp =
            new BootstrapParams(snp.getRootDirPath(),
                                snp.getHostname(),
                                snp.getHAHostname(),
                                snp.getHAPortRange(),
                                gp.getKVStoreName(),
                                snp.getRegistryPort(),
                                adminPort,
                                snp.getCapacity(),
                                snp.getMountPoints(),
                                isSecured);
        bp.setHostingAdmin(hostingAdmin);

        /*
         * Authentication implies a secured store, so we need to set the
         * security directory in the BootStrapParams.
         */
        if (isSecured) {
            if (secDir == null || secDir.isEmpty()) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }
            bp.setSecurityDir(secDir);
        }
        ParameterMap map = bp.getMap();
        map.setParameter(ParameterState.COMMON_SN_ID,
                         Integer.toString(snp.getStorageNodeId().
                                          getStorageNodeId()));
        lp.addMap(map);
        lp.setVersion(ParameterState.BOOTSTRAP_PARAMETER_VERSION);
        return lp;
    }

    /**
     * Generate the expected file hierarchy under the root
     */
    private void createFiles(String rootName,
                             String storeName,
                             LoadParameters lp,
                             LoadParameters bootLp)
        throws Exception {

        String secPolicy = "grant {\n  permission java.security.AllPermission;\n};\n";
        String tmpName = rootName;
        File rootDir = new File(tmpName);
        rootDir.mkdir();
        bootLp.saveParameters(new File(rootDir, "config.xml"));
        File storeDir = new File(rootDir, storeName);
        storeDir.mkdir();
        File snDir = FileNames.getStorageNodeDir(storeDir, snid);
        snDir.mkdir();
        writeFile(new File(rootDir, "security.policy"), secPolicy);
        writeFile(new File(storeDir, "security.policy"), secPolicy);
        lp.saveParameters(FileNames.getSNAConfigFile(rootDir.getAbsolutePath(),
                                                     storeName, snid));
    }

    private void writeFile(File file, String content) {
        PrintWriter writer = null;
        try {
            FileOutputStream fos = new FileOutputStream(file);
            writer = new PrintWriter(fos);
            writer.printf("%s", content);
        } catch (Exception e) {
            throw new IllegalStateException("Problem saving file: " +
                                            file + ": " + e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
     * Create the archive, recursively.  Allow the specified target to include
     * ".zip."  Add it if not.
     */
    private void createZip(String rootName) {
        try {
            String targetName = target;
            if (!targetName.contains(".zip")) {
                targetName += ".zip";
            }
            FileOutputStream fout = new FileOutputStream(new File(targetName));
            ZipOutputStream zout = new ZipOutputStream(fout);
            System.out.println("Creating zipfile " + targetName);
            File source = new File(rootName);
            addDirectory(zout, source);
            zout.close();
        } catch (IOException ioe) {
            System.out.println("IOException :" + ioe);
        }
    }

    private void addDirectory(ZipOutputStream zout, File source) {

        File[] files = source.listFiles();
        System.out.println("Adding directory " + source.getPath());
        for (int i = 0; i < files.length; ++i) {
            if (files[i].isDirectory()) {
                addDirectory(zout, new File(source.getPath() +
                                            File.separator + files[i].getName()));
                continue;
            }
            try {
                String path = source.getPath() + File.separator + files[i].getName();
                System.out.println("Adding file " + path);
                byte[] buffer = new byte[1024];
                FileInputStream fin = new FileInputStream(path);
                zout.putNextEntry(new ZipEntry(path));
                int length;
                while((length = fin.read(buffer)) > 0) {
                    zout.write(buffer, 0, length);
                }
                zout.closeEntry();
                fin.close();
            } catch (IOException ioe) {
                System.out.println("IOException :" + ioe);
            }
        }
    }

    static void delete(File f)
        throws IOException {

        if (f.isDirectory()) {
            for (File file : f.listFiles())
                delete(file);
        }
        f.delete();
    }

    private void initLogin()
        throws IOException {

        final KVStoreLogin storeLogin =
            new KVStoreLogin(userName, securityFilePath);
        storeLogin.loadSecurityProperties();
        storeLogin.prepareRegistryCSF();
        isSecured = storeLogin.foundSSLTransport();
        if (isSecured) {
            pwdCreds = storeLogin.makeShellLoginCredentials();
        }
    }

    /**
     * Command line parsing
     */
    class Parser extends CommandParser {
        public static final String snidFlag = "-sn";
        public static final String targetFlag = "-target";
        public static final String secDirFlag = "-secdir";
        public Parser(String[] args) {
            super(args);
        }

        @Override
        protected void verifyArgs() {
            if (getHostname() == null ||
                getRegistryPort() == 0 ||
                snid == null ||
                target == null) {
                usage("Missing required argument");
            }
        }

        @Override
        public void usage(String errorMsg) {
            if (errorMsg != null) {
                System.err.println(errorMsg);
            }
            System.err.println(KVSTORE_USAGE_PREFIX + COMMAND_NAME +
                               "\n\t" + COMMAND_ARGS);
            System.exit(-1);
        }

        @Override
        protected boolean checkArg(String arg) {
            if (arg.equals(snidFlag)) {
                String sn = nextArg(arg);
                try {
                    snid = StorageNodeId.parse(sn);
                } catch (IllegalArgumentException iae) {
                    usage("Invalid Storage Node Id: " + sn);
                }
                return true;
            }
            if (arg.equals(targetFlag)) {
                target = nextArg(arg);
                return true;
            }
            if (arg.equals(secDirFlag)) {
                secDir = nextArg(arg);
                return true;
            }
            return false;
        }
    }

    public void parseArgs(String args[]) {
        parser = new Parser(args);
        parser.parseArgs();
        userName = parser.getUserName();
        securityFilePath = parser.getSecurityFile();
    }

    public static void main(String[] args) {
        GenerateConfig gen = new GenerateConfig();
        gen.parseArgs(args);
        try {
            gen.generate();
        } catch (Exception e) {
            System.err.println("Exception in GenerateConfig: " + e);
        }
    }
}
