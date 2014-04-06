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

package oracle.kv.impl.admin.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;

import oracle.kv.impl.admin.AdminFaultException;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.avro.AvroDdl.AddSchemaOptions;
import oracle.kv.impl.api.avro.AvroDdl.AddSchemaResult;
import oracle.kv.impl.api.avro.AvroSchemaMetadata;
import oracle.kv.impl.api.avro.AvroSchemaStatus;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

import org.apache.avro.Schema;

/*
 * Subcommands of snapshot
 *   create
 *   remove
 */
class DdlCommand extends CommandWithSubs {

    private static final
        List<? extends SubCommand> subs =
                       Arrays.asList(new DdlAddSub(),
                                     new DdlEnableSub(),
                                     new DdlDisableSub()
                                     );

    DdlCommand() {
        super(subs, "ddl", 3, 3);
    }

    @Override
    protected String getCommandOverview() {
        return "Encapsulates operations that manipulate schemas in the store.";
    }

    private static class DdlAddSub extends SubCommand {

        private DdlAddSub() {
            super("add-schema", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String fileName = null;
            String schemaString = null;
            boolean evolve = false;
            boolean force = false;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-file".equals(arg)) {
                    fileName = Shell.nextArg(args, i++, this);
                } else if ("-string".equals(arg)) {
                    schemaString = Shell.nextArg(args, i++, this);
                } else if ("-evolve".equals(arg)) {
                    evolve = true;
                } else if ("-force".equals(arg)) {
                    force = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (fileName == null && schemaString == null) {
                shell.requiredArg("-file|-string", this);
            }

            try {
                /* If reading from file, read.  Assumes default encoding. */
                if (schemaString == null) {
                    final BufferedReader reader;
                    try {
                        reader = new BufferedReader(new FileReader(fileName));
                    } catch (FileNotFoundException e) {
                        return "File not found: " + fileName;
                    }
                    final StringBuilder schemaText =
                        new StringBuilder((int) (new File(fileName).length()));
                    while (true) {
                        final String line;
                        try {
                            line = reader.readLine();
                        } catch (IOException e) {
                            return
                                "Unable to read file (" + e.getMessage() + ")";
                        }
                        if (line == null) {
                            break;
                        }
                        schemaText.append(line).append(eol);
                    }
                    schemaString = schemaText.toString();
                }
                /*
                 * Parse schema here (prior to calling addSchema in the
                 * service) to get an error message referring to the local
                 * file, and to get the name of the schema.  The service
                 * addSchema method will parse it again, and check for
                 * evolution errors as well.
                 */
                final String schemaName;
                try {
                    final Schema schema =
                        new Schema.Parser().parse(schemaString);
                    schemaName = schema.getFullName();
                } catch (Exception e) {
                    return e.getMessage();
                }

                /* Add the schema as initially active/enabled. */
                final AvroSchemaMetadata metadata =
                    makeAvroSchemaMetadata(AvroSchemaStatus.ACTIVE);
                try {
                    final AddSchemaOptions options =
                        new AddSchemaOptions(evolve, force);
                    final AddSchemaResult result =
                        cs.addSchema(metadata, schemaString, options);
                    final String resultMsg = "Added schema: " + schemaName +
                                             "." + result.getId();
                    final String extraMsg = result.getExtraMessage();
                    if (extraMsg != null && extraMsg.length() > 0) {
                        return resultMsg + eol + extraMsg;
                    }
                    return resultMsg;
                } catch (AdminFaultException afe) {
                    shell.handleUnknownException("Failed to add schema", afe);
                }
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "ddl add-schema <-file <file> | " +
                "-string <schema string>>" + eolt + "[-evolve] [-force]";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Adds a new schema or changes (evolves) an existing schema " +
                "with the same" + eolt + "name.  The -evolve flag is used " +
                "to indicate that the schema is changing." + eolt + "The " +
                "-force flag adds the schema in spite of evolution warnings.";
        }
    }

    private static class DdlEnableSub extends DdlEnableDisableSub {

        private DdlEnableSub() {
            super("enable-schema", 3, true);
        }

        @Override
        protected String getCommandSyntax() {
            return "ddl enable-schema -name <name>.<ID>";
        }

        @Override
        protected String getCommandDescription() {
            return "Enables an existing, previously disabled schema";
        }
    }

    private static class DdlDisableSub extends DdlEnableDisableSub {

        private DdlDisableSub() {
            super("disable-schema", 3, false);
        }

        @Override
        protected String getCommandSyntax() {
            return "ddl disable-schema -name <name>.<ID>";
        }

        @Override
        protected String getCommandDescription() {
            return "Disables an existing schema";
        }
    }

    private static abstract class DdlEnableDisableSub extends SubCommand {
        private final boolean isEnable;

        protected DdlEnableDisableSub(String name, int prefixMatchLength,
                                      boolean isEnable) {
            super(name, prefixMatchLength);
            this.isEnable = isEnable;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            CommandShell cmd = (CommandShell)shell;
            CommandServiceAPI cs = cmd.getAdmin();
            String schemaName = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-name".equals(arg)) {
                    schemaName = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (schemaName == null) {
                shell.requiredArg("-name", this);
            }

            try {
                AvroSchemaStatus status =
                    (isEnable ? AvroSchemaStatus.ACTIVE :
                     AvroSchemaStatus.DISABLED);
                final int id;
                id = ShowCommand.ShowSchemas.parseSchemaNameAndId
                    (schemaName, true /*idRequired*/, cs, this);
                final boolean updated =
                    cs.updateSchemaStatus(id, makeAvroSchemaMetadata(status));
                return
                    (updated ? "Status updated to " : "Status was already ") +
                    status;
            } catch (RemoteException re) {
                cmd.noAdmin(re);
            }
            return "";
        }
    }

    protected static AvroSchemaMetadata
        makeAvroSchemaMetadata(AvroSchemaStatus status) {

        return new AvroSchemaMetadata(status, System.currentTimeMillis(),
                                      getAdminUser(), getAdminMachine());
    }

    protected static String getAdminUser() {
        return "";
    }

    protected static String getAdminMachine() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "";
        }
    }
}
