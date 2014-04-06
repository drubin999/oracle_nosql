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
import java.util.ArrayList;
import java.util.List;

import oracle.kv.impl.admin.plan.Plan;

import com.sleepycat.persist.model.Persistent;

/**
 * A list of tasks that is used to carry out a {@link Plan}.  In R1, Plans will
 * consist of a single TaskList with a {@link ExecutionStrategy#SERIAL}
 * execution strategy.
 *
 * In R2, TaskLists will extend Task and will be nestable. A nested TaskList
 * within a Plan would permit parallelizable execution. For example,
 *      TaskA
 *       |
 *     TaskListB  <--- represents parallel execution
 *       |
 *      Task C
 */
@Persistent
public class TaskList implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The options for how this list of tasks is to be executed: in serial
     * or in parallel.
     */
    public enum ExecutionStrategy {
        /** Tasks can be executed in parallel, or serially */
        PARALLEL,
        /** Tasks just be executed serially */
        SERIAL
    }

    /**
     * The strategy to use for executing this task list. 
     */
    private /*final*/ ExecutionStrategy strategy;

    /**
     * The ordered list of tasks to perform. Defined as ArrayList to support
     * cloning.
     */
    private /*final*/ ArrayList<Task> taskList;

    public TaskList(ExecutionStrategy execOrder) {
        taskList = new ArrayList<Task>();
        strategy = execOrder;
    }

    /*
     * No-arg ctor for use by DPL.
     */
    @SuppressWarnings("unused")
    private TaskList() {
    }

    public void add(Task t) {
        taskList.add(t);
    }

    /**
     * Returns a cloned task list, which can be used for execution. Because
     * it's a shallow copy, the task attributes such as state may be changed
     * and will be visible to other accessors of the task, which is the 
     * intended effect.
     */
    @SuppressWarnings("unchecked")
    public List<Task> getTasks() {
        return (List<Task>) taskList.clone();
    }

    /**
     * Get the number of all tasks to be done. Because of nested tasks, there
     * may be more tasks than 1. If this is a nested task, the umbrella task
     * has no work, so one only counts the nested tasks.
     */
    public int getTotalTaskCount() {
        int count = 0;
        for (Task t: taskList) {
            count += t.getTotalTaskCount();
        }
        return count;
    }

    /**
     * Returns true if this list is empty.
     * @return true if this list is empty
     */
    public boolean isEmpty() {
        return taskList.isEmpty();
    }

    public ExecutionStrategy getStrategy() {
        return strategy;
    }
}
