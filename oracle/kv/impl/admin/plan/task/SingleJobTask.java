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

import java.util.concurrent.Callable;

import com.sleepycat.persist.model.Persistent;

import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;

/**
 * A task that is meant to be executed in a single phase of work.
 */
@Persistent
public abstract class SingleJobTask extends AbstractTask {

    private static final long serialVersionUID = 1L;

    /**
     * Contains the all the logic needed to execute the task, to be done as
     * a single job, or phase.
     */
    public abstract Task.State doWork() throws Exception;

    /**
     * Task execution is started off with a Callable that encompasses
     * all the work of the task.
     */
    @Override
    public Callable<Task.State>
        getFirstJob(int taskNum, ParallelTaskRunner unused) {

        return new Callable<Task.State>() {
            @Override
            public State call() throws Exception {
                return doWork();
            }
        };
    }
}
