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

import java.io.Serializable;
import java.util.Map;

import oracle.kv.impl.admin.plan.Planner;

import com.sleepycat.persist.model.Persistent;

/**
 * A common base class for implementations of the {@link Task} interface.
 */
@Persistent
public abstract class AbstractTask implements Task, Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * If a task cleanup job fails, keep retrying periodically, until it either
     * succeeds or the user interrupts the plan again.
     */
    static final int CLEANUP_RETRY_MILLIS = 120000;

    public AbstractTask() {
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public TaskList getNestedTasks() {
        /* A task list is only returned for parallel tasks. */
        return null;
    }

    /**
     * AbstractTasks are assumed to not have any nested tasks.
     */
    @Override
    public int getTotalTaskCount() {
        return 1;
    }

    /**
     * Most tasks have no cleanup to do. Whatever changes they have executed
     * can be left untouched.
     */
    @Override
    public Runnable getCleanupJob() {
        return null;
    }

    /**
     * Obtain any required topo locks before plan execution, to avoid conflicts
     * in concurrent plans.
     */
    @Override
    public void lockTopoComponents(Planner planner) {
        /* default: no locks needed */
    }
    
    /*
     * Format any detailed information collected about the task in a way
     * that's usable for plan reporting. Should be overridden by tasks to 
     * provide customized status reports.
     * 
     * @return null if there are no details.
     */
    @Override
    public String displayExecutionDetails(Map<String, String> details,
                                          String displayPrefix) {
        if (details.isEmpty()) {
            return null;
        }
        
        return details.toString();
    }
}
