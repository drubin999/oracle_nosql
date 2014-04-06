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

import static oracle.kv.impl.security.PasswordManager.FILE_STORE_MANAGER_CLASS;
import static oracle.kv.impl.security.PasswordManager.WALLET_MANAGER_CLASS;

import java.io.File;
import java.util.Arrays;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.security.PasswordManager;
import oracle.kv.impl.security.PasswordStoreException;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig;
import oracle.kv.impl.util.SecurityConfigCreator.ShellIOHelper;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

class SecurityConfigCommand extends CommandWithSubs {

    static final String ROOT_FLAG = "-root";
    static final String PWDMGR_FLAG = "-pwdmgr";
    static final String PASSMGR_WALLET = "wallet";
    static final String PASSMGR_PWDFILE = "pwdfile";
    static final String KEYSTORE_PASSWORD_FLAG = "-kspwd";
    static final String SECURITY_DIR_FLAG = "-secdir";
    static final String CERT_MODE_FLAG = "-certmode";
    static final String SECURITY_PARAM_FLAG = "-security-param";
    static final String PARAM_FLAG = "-param";
    static final String CONFIG_FLAG = "-config";
    static final String SOURCE_ROOT_FLAG = "-source-root";
    static final String SOURCE_SECURITY_DIR_FLAG = "-source-secdir";

    private static final String BASIC_CREATE_COMMAND_ARGS =
        "[-secdir <security dir>] " +
        "[-pwdmgr {pwdfile | wallet | <class-name>}] " + "\n\t" +
        "[-kspwd <password>] " +
        "[-param <param=value>]*";
    private static final String CREATE_COMMAND_ARGS =
        "-root <secroot> " + "\n\t" + BASIC_CREATE_COMMAND_ARGS;
    /* Not currently documented - shared is default */
    @SuppressWarnings("unused")
    private static final String OTHER_ARGS =
        " [-certmode { shared | server }] ";
    private static final String ADD_SECURITY_COMMAND_ARGS =
        "-root <kvroot> " +
        "[-secdir <security dir>] " +
        "[-config <config.xml>]";
    private static final String REMOVE_SECURITY_COMMAND_ARGS =
        "-root <kvroot> " +
        "[-config <config.xml>]";
    private static final String MERGE_TRUST_COMMAND_ARGS =
        "-root <secroot> " +
        "[-secdir <security dir>] " +
        "-source-root <source secroot> " +
        "[-source-secdir <source secdir>]";

    SecurityConfigCommand() {
        super(Arrays.asList(new SecurityConfigCreate(),
                            new SecurityConfigAddSecurity(),
                            new SecurityConfigRemoveSecurity(),
                            new SecurityConfigMergeTrust()),
              "config", 4, 1);
    }

    @Override
    public String getCommandOverview() {
        return "The config command allows configuration of security settings " +
            "for a NoSQL installation.";
    }

    public static String getPwdmgrClass(String pwdmgr) {
        if (pwdmgr == null) {
            return PasswordManager.preferredManagerClass();
        }
        if (pwdmgr.equals(PASSMGR_PWDFILE)) {
            return FILE_STORE_MANAGER_CLASS;
        }
        if (pwdmgr.equals(PASSMGR_WALLET)) {
            return WALLET_MANAGER_CLASS;
        }
        return pwdmgr;
    }

    /**
     * Parsing logic for configuration creation used by makebootconfig.
     */
    static class ConfigParserHelper {
        private final CommandParser parser;
        private final ParsedConfig config;

        public ConfigParserHelper(CommandParser parser) {
            this.parser = parser;
            this.config = new ParsedConfig();
        }

        public boolean checkArg(String arg) {
            if (arg.equals(PWDMGR_FLAG)) {
                config.setPwdmgr(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(KEYSTORE_PASSWORD_FLAG)) {
                config.setKeystorePassword(parser.nextArg(arg).toCharArray());
                return true;
            }
            if (arg.equals(SECURITY_DIR_FLAG)) {
                config.setSecurityDir(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(CERT_MODE_FLAG)) {
                config.setCertMode(parser.nextArg(arg));
                return true;
            }
            if (arg.equals(SECURITY_PARAM_FLAG)) {
                try {
                    config.addParam(parser.nextArg(arg));
                } catch (IllegalArgumentException iae) {
                    parser.usage("invalid argument usage for " + arg +
                                 " - " + iae.getMessage());
                }
                return true;
            }
            return false;
        }

        public ParsedConfig getConfig() {
            return config;
        }

        /**
         * For makebootconfig, don't report arguments that are supported
         * by the core code.
         */
        public static String getConfigUsage() {
            return BASIC_CREATE_COMMAND_ARGS;
        }
    }

    /**
     * SecurityConfigCreate - implements the "create" subcommand.
     */
    private static final class SecurityConfigCreate extends SubCommand {
        private static final String CREATE_COMMAND_NAME = "create";
        private static final String CREATE_COMMAND_DESC =
            "Creates a new security configuration.";

        private SecurityConfigCreate() {
            super(CREATE_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            final ParsedConfig config = new ParsedConfig();
            String kvRoot = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (PWDMGR_FLAG.equals(arg)) {
                    config.setPwdmgr(Shell.nextArg(args, i++, this));
                } else if (KEYSTORE_PASSWORD_FLAG.equals(arg)) {
                    config.setKeystorePassword(
                        Shell.nextArg(args, i++, this).toCharArray());
                } else if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    config.setSecurityDir(Shell.nextArg(args, i++, this));
                } else if (CERT_MODE_FLAG.equals(arg)) {
                    config.setCertMode(Shell.nextArg(args, i++, this));
                } else if (PARAM_FLAG.equals(arg)) {
                    try {
                        config.addParam(Shell.nextArg(args, i++, this));
                    } catch (IllegalArgumentException iae) {
                        shell.badArgUsage(arg, iae.getMessage(), this);
                    }
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doCreate(kvRoot, config, shell);
        }

        private String doCreate(String kvRoot,
                                ParsedConfig config,
                                Shell shell)
            throws ShellException {

            final SecurityConfigCreator creator =
                new SecurityConfigCreator(kvRoot,
                                          config,
                                          new ShellIOHelper(shell));

            try {
                if (creator.createConfig()) {
                    return "Created";
                }
                return "Failed";
            } catch (PasswordStoreException pwse) {
                throw new ShellException(
                    "PasswordStore error: " + pwse.getMessage(), pwse);
            } catch (Exception e) {
                throw new ShellException(
                    "Unknown error: " + e.getMessage(), e);
            }
        }

        @Override
        public String getCommandSyntax() {
            return "config create " + CREATE_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return CREATE_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigAddSecurity - implements the "add-security" subcommand.
     */
    private static final class SecurityConfigAddSecurity extends SubCommand {
        private static final String ADD_SECURITY_COMMAND_NAME = "add-security";
        private static final String ADD_SECURITY_COMMAND_DESC =
            "Updates a Storage Node configuration to incorporate a security " +
            "configuartion.";

        private SecurityConfigAddSecurity() {
            super(ADD_SECURITY_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String configXml = null;
            String secDir = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    secDir = Shell.nextArg(args, i++, this);
                } else if (CONFIG_FLAG.equals(arg)) {
                    configXml = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doAddSecurity(kvRoot, configXml, secDir);
        }

        private String doAddSecurity(String kvRoot,
                                     String configXml,
                                     String secDir)
            throws ShellException {

            if (configXml == null) {
                configXml = FileNames.SNA_CONFIG_FILE;
            }

            if (secDir == null) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }

            final File kvRootFile = new File(kvRoot);
            final File configXmlFile = new File(configXml);
            final File secDirFile = new File(secDir);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (configXmlFile.isAbsolute()) {
                throw new ShellException(
                    "The -config argument must be a relative file name.");
            }

            final File rootedConfigXmlFile = new File(kvRootFile, configXml);
            if (!rootedConfigXmlFile.exists()) {
                throw new ShellException(
                    "The file " + configXml + " does not exist in " + kvRoot);
            }

            if (!rootedConfigXmlFile.isFile()) {
                throw new ShellException(
                    rootedConfigXmlFile.toString() + " is not a file.");
            }

            if (secDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -secdir argument must be a relative file name.");
            }

            final File rootedSecDirFile = new File(kvRootFile, secDir);
            if (!rootedSecDirFile.exists()) {
                throw new ShellException(
                    "The file " + secDir + " does not exist in " + kvRoot);
            }

            if (!rootedSecDirFile.isDirectory()) {
                throw new ShellException(
                    rootedSecDirFile.toString() + " is not a directory.");
            }

            updateConfigFile(rootedConfigXmlFile, secDirFile);
            return "Configuration updated.";
        }

        private void updateConfigFile(File configFile, File secDir)
            throws ShellException {

            final BootstrapParams bp =
                ConfigUtils.getBootstrapParams(configFile);
            if (bp == null) {
                throw new ShellException(
                    "The file " + configFile +
                    " does not contain a bootstrap configuration.");
            }
            bp.setSecurityDir(secDir.getPath());

            /*
             * Check whether the configuration has a sufficiently large service
             * port range allocated, if set.
             */
            try {
                checkSufficientPorts(bp);
            } catch (IllegalArgumentException iae) {
                throw new ShellException(
                    "The configuration will not work in a secure " +
                    " environment. Please adjust the configuration before " +
                    " enabling security. (" + iae.getMessage() + ")");
            }

            ConfigUtils.createBootstrapConfig(bp, configFile);
        }

        /**
         * Check whether the bootstrap config has a sufficient portrange
         * available, assuming that is is a secure config.
         * @param bp the bootstrap config to consider
         * @throws IllegalArgumentException if the configuration is not valid
         */
        private void checkSufficientPorts(BootstrapParams bp) {
            final String servicePortRange = bp.getServicePortRange();
            if (servicePortRange != null && !servicePortRange.isEmpty()) {
                final int adminHttpPort = bp.getAdminHttpPort();
                PortRange.validateSufficientPorts(servicePortRange,
                                                  bp.getCapacity(),
                                                  true,
                                                  (adminHttpPort != 0 ?
                                                   true : false),
                                                  bp.getMgmtPorts());
            }
        }

        @Override
        public String getCommandSyntax() {
            return "config add-security " + ADD_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return ADD_SECURITY_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigRemoveSecurity - implements the "remove-security"
     * subcommand.
     */
    private static final class SecurityConfigRemoveSecurity extends SubCommand {
        private static final String REMOVE_SECURITY_COMMAND_NAME =
            "remove-security";
        private static final String REMOVE_SECURITY_COMMAND_DESC =
            "Updates a Storage Node configuration to remove the security " +
            "configuartion.";

        private SecurityConfigRemoveSecurity() {
            super(REMOVE_SECURITY_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String configXml = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (CONFIG_FLAG.equals(arg)) {
                    configXml = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            return doRemoveSecurity(kvRoot, configXml);
        }

        private String doRemoveSecurity(String kvRoot,
                                        String configXml)
            throws ShellException {

            if (configXml == null) {
                configXml = FileNames.SNA_CONFIG_FILE;
            }

            final File kvRootFile = new File(kvRoot);
            final File configXmlFile = new File(configXml);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (configXmlFile.isAbsolute()) {
                throw new ShellException(
                    "The -config argument must be a relative file name.");
            }

            final File rootedConfigXmlFile = new File(kvRootFile, configXml);
            if (!rootedConfigXmlFile.exists()) {
                throw new ShellException(
                    "The file " + configXml + " does not exist in " + kvRoot);
            }

            if (!rootedConfigXmlFile.isFile()) {
                throw new ShellException(
                    rootedConfigXmlFile.toString() + " is not a file.");
            }

            updateConfigFile(rootedConfigXmlFile);
            return "Configuration updated.";
        }

        private void updateConfigFile(File configFile)
            throws ShellException {

            final BootstrapParams bp =
                ConfigUtils.getBootstrapParams(configFile);
            if (bp == null) {
                throw new ShellException(
                    "The file " + configFile +
                    " does not contain a bootstrap configuration.");
            }
            final String secDir = bp.getSecurityDir();
            if (secDir == null) {
                throw new ShellException(
                    "The file " + configFile +
                    " does not currently have security configured.");
            }
            bp.setSecurityDir(null);

            ConfigUtils.createBootstrapConfig(bp, configFile);
        }

        @Override
        public String getCommandSyntax() {
            return "config remove-security " + REMOVE_SECURITY_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return REMOVE_SECURITY_COMMAND_DESC;
        }
    }

    /**
     * SecurityConfigMergeTrust - implements the "merge-trust" subcommand.
     */
    private static final class SecurityConfigMergeTrust extends SubCommand {
        private static final String MERGE_TRUST_COMMAND_NAME = "merge-trust";
        private static final String MERGE_TRUST_COMMAND_DESC =
            "Merges trust information from a source security directory into " +
            "a security configuration.";

        private SecurityConfigMergeTrust() {
            super(MERGE_TRUST_COMMAND_NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String kvRoot = null;
            String secDir = null;
            String srcKvRoot = null;
            String srcSecDir = null;

            for (int i = 1; i < args.length; i++) {
                final String arg = args[i];
                if (ROOT_FLAG.equals(arg)) {
                    kvRoot = Shell.nextArg(args, i++, this);
                } else if (SECURITY_DIR_FLAG.equals(arg)) {
                    secDir = Shell.nextArg(args, i++, this);
                } else if (SOURCE_ROOT_FLAG.equals(arg)) {
                    srcKvRoot = Shell.nextArg(args, i++, this);
                } else if (SOURCE_SECURITY_DIR_FLAG.equals(arg)) {
                    srcSecDir = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (kvRoot == null) {
                shell.requiredArg(ROOT_FLAG, this);
            }

            if (srcKvRoot == null) {
                shell.requiredArg(SOURCE_ROOT_FLAG, this);
            }

            return doMergeTrust(kvRoot, secDir, srcKvRoot, srcSecDir);
        }

        private String doMergeTrust(String kvRoot,
                                    String secDir,
                                    String srcKvRoot,
                                    String srcSecDir)
            throws ShellException {

            if (secDir == null) {
                secDir = FileNames.SECURITY_CONFIG_DIR;
            }

            if (srcSecDir == null) {
                srcSecDir = FileNames.SECURITY_CONFIG_DIR;
            }

            final File kvRootFile = new File(kvRoot);
            final File secDirFile = new File(secDir);
            final File srcKvRootFile = new File(srcKvRoot);
            final File srcSecDirFile = new File(srcSecDir);

            if (!kvRootFile.exists()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile + " does not exist.");
            }

            if (!kvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -root argument " + kvRootFile +
                    " is not a directory.");
            }

            if (secDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -secdir argument must be a relative file name.");
            }

            final File rootedSecDirFile = new File(kvRootFile, secDir);
            if (!rootedSecDirFile.exists()) {
                throw new ShellException(
                    "The file " + secDir + " does not exist in " + kvRoot);
            }

            if (!rootedSecDirFile.isDirectory()) {
                throw new ShellException(
                    rootedSecDirFile.toString() + " is not a directory.");
            }

            if (!srcKvRootFile.exists()) {
                throw new ShellException(
                    "The -source-root argument " + srcKvRootFile +
                    " does not exist.");
            }

            if (!srcKvRootFile.isDirectory()) {
                throw new ShellException(
                    "The -source-root argument " + srcKvRootFile +
                    " is not a directory.");
            }

            if (srcSecDirFile.isAbsolute()) {
                throw new ShellException(
                    "The -source-secdir argument must be a " +
                    "relative file name.");
            }

            final File rootedSrcSecDirFile = new File(srcKvRootFile, srcSecDir);
            if (!rootedSrcSecDirFile.exists()) {
                throw new ShellException(
                    "The file " + srcSecDir + " does not exist in " +
                    srcKvRoot);
            }

            if (!rootedSrcSecDirFile.isDirectory()) {
                throw new ShellException(
                    rootedSrcSecDirFile.toString() + " is not a directory.");
            }

            SecurityUtils.mergeTrust(rootedSrcSecDirFile, rootedSecDirFile);
            return "Configuration updated.";
        }

        @Override
        public String getCommandSyntax() {
            return "config merge-trust " + MERGE_TRUST_COMMAND_ARGS;
        }

        @Override
        public String getCommandDescription() {
            return MERGE_TRUST_COMMAND_DESC;
        }
    }
}
