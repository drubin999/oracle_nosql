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

package oracle.kv.impl.admin;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import oracle.kv.impl.admin.VerifyConfiguration.Problem;

/**
 * Return progress information and results from a verification run.
 */
public class VerifyResults implements Serializable {
    private static final String eol = System.getProperty("line.separator");
    private static final long serialVersionUID = 1L;
    private final List<Problem> violations;
    private final List<Problem> warnings;
    private final String progressReport;

    VerifyResults(String progressReport,
                  List<Problem> violations,
                  List<Problem> warnings) {
        this.progressReport = progressReport;
        this.violations = violations;
        this.warnings = warnings;
    }

    VerifyResults(List<Problem> violations,
                  List<Problem> warnings) {
        this.progressReport = null;
        this.violations = violations;
        this.warnings = warnings;
    }

    public String getProgressReport() {
        return progressReport;
    }

    public int numWarnings() {
        return warnings.size();
    }

    public List<Problem> getViolations() {
        return violations;
    }

    public int numViolations() {
        return violations.size();
    }

    public List<Problem> getWarnings() {
        return warnings;
    }

    public boolean okay() {
        return (violations.size() == 0) && (warnings.size() == 0);
    }

    public String display() {
        int numViolations = violations.size();
        int numWarnings = warnings.size();

        if ((numViolations + numWarnings) == 0) {
            return "Verification complete, no violations.";
        }

        StringBuilder sb = new StringBuilder();

        sb.append("Verification complete, ").append(numViolations);
        if (numViolations == 1) {
            sb.append(" violation, ");
        } else {
            sb.append(" violations, ");
        }

        sb.append(numWarnings);
        if (numWarnings == 1) {
            sb.append(" note");
        } else {
            sb.append(" notes");
        }

        sb.append(" found.").append(eol);

        /* Show the violations forst, and then the warnings. */
        Comparator<Problem> resourceComparator =
            new Comparator<Problem>() {
            @Override
            public int compare(Problem p1, Problem p2) {
                return p1.getResourceId().toString().compareTo
                (p2.getResourceId().toString());
            }
        };

        Collections.sort(violations, resourceComparator);
        Collections.sort(warnings, resourceComparator);

        for (Problem problem : violations) {
            sb.append("Verification violation: [").
                append(problem.getResourceId()).append("]\t").
                append(problem).append(eol);
        }

        for (Problem problem : warnings) {
            sb.append("Verification note: [").
                append(problem.getResourceId()).append("]\t").
                append(problem).append(eol);
        }

        return sb.toString();
    }
}
