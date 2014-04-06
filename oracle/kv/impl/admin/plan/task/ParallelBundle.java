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

import oracle.kv.impl.admin.plan.PlanExecutor.ParallelTaskRunner;
import oracle.kv.impl.admin.plan.task.TaskList.ExecutionStrategy;
import com.sleepycat.persist.model.Persistent;

/**
 * Groups together a set of nested tasks for parallel execution.
 */
@Persistent
public class ParallelBundle extends AbstractTask {

    private static final long serialVersionUID = 1L;

    private TaskList taskList;

    public ParallelBundle() {
        taskList = new TaskList(ExecutionStrategy.PARALLEL);
    }

    @Override
    public TaskList getNestedTasks() {
        return taskList;
    }

    public void addTask(Task task) {
        taskList.add(task);
    }

    /**
     * Return the number of nested tasks.
     */
    @Override
    public int getTotalTaskCount() {
        return taskList.getTotalTaskCount();
    }

    /**
     * Returns true if this bundle is empty.
     * @return true if this bundle is empty
     */
    public boolean isEmpty() {
        return taskList.isEmpty();
    }

    /**
     * No work is done in this task. Its only purpose is to shelter its nested
     * tasks.
     */
    @Override
    public Callable<Task.State> getFirstJob(int taskId, 
                                            ParallelTaskRunner runner)
        throws Exception {
        
        throw new UnsupportedOperationException
           ("Should be no work of its own in the parent task");
    }

    @Override
    public boolean continuePastError() {
        return true;
    }
}
