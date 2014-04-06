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

import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.DatacenterId;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellUsageException;

/** A subclass of {@link CommandWithSubs} that provides shared utilities. */
abstract class SharedCommandWithSubs extends CommandWithSubs {

    protected SharedCommandWithSubs(
        final List<? extends SubCommand> subCommands,
        final String name,
        final int prefixLength,
        final int minArgCount) {

        super(subCommands, name, prefixLength, minArgCount);
    }

    /** A subclass of {@link SubCommand} that provides shared utilities. */
    static abstract class SharedSubCommand extends SubCommand {
        protected SharedSubCommand(final String name, final int prefixLength) {
            super(name, prefixLength);
        }

        protected void validateRepNodes(CommandServiceAPI cs,
                                        Set<RepNodeId> rnids)
            throws RemoteException, ShellException {

            for (RepNodeId rnid : rnids) {
                CommandUtils.ensureRepNodeExists(rnid, cs, this);
            }
        }

        protected int parseInt(String idString, String msg)
            throws ShellException {

            try {
                return Integer.parseInt(idString);
            } catch (IllegalArgumentException ignored) {
                throw new ShellUsageException
                    (msg + ": " + idString, this);
            }
        }

        protected StorageNodeId parseSnid(String idString)
            throws ShellException {

            try {
                return StorageNodeId.parse(idString);
            } catch (IllegalArgumentException ignored) {
                throw new ShellUsageException
                    ("Invalid storage node ID: " + idString, this);
            }
        }

        protected DatacenterId parseDatacenterId(String idString)
            throws ShellException {

            try {
                return DatacenterId.parse(idString);
            } catch (IllegalArgumentException ignored) {
                throw new ShellUsageException
                    ("Invalid zone ID: " + idString, this);
            }
        }

        protected RepNodeId parseRnid(String idString)
            throws ShellException {

            try {
                return RepNodeId.parse(idString);
            } catch (IllegalArgumentException ignored) {
                throw new ShellUsageException
                    ("Invalid RepNode ID: " + idString, this);
            }
        }

        protected AdminId parseAdminid(String idString)
            throws ShellException {

            try {
                return AdminId.parse(idString);
            } catch (IllegalArgumentException ignored) {
                throw new ShellUsageException
                    ("Invalid Admin ID: " + idString, this);
            }
        }

        protected DatacenterType parseDatacenterType(final String string)
            throws ShellException {

            try {
                return DatacenterType.valueOf(string.toUpperCase());
            } catch (IllegalArgumentException ignored) {
                throw new ShellUsageException(
                    "Invalid zone type: " + string, this);
            }
        }
    }
}
