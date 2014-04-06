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
package oracle.kv.impl.security.util;

import java.io.File;
import java.io.IOException;

/*
 * TODO: consider using the java.nio.file functionality present in Java 7+
 */
/**
 * A collection of File-system permission utility functions.
 */
public final class FileSysUtils {

    /*
     * Not instantiable
     */
    private FileSysUtils() {
    }

    public static Operations selectOsOperations() {
        final String os = System.getProperty("os.name").toLowerCase();

        if (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 ||
            os.indexOf("aix") > 0  || os.indexOf("sunos") >= 0) {
            return new JavaAPIOperations();
        }

        if (os.indexOf("win") >= 0) {
            return new WindowsCmdLineOperations();
        }

        return new JavaAPIOperations();
    }

    public interface Operations {
        /**
         * Given an abstract file, attempt to change permissions so that it
         * is readable only by the owner of the file.
         * @param f a File referencing a file or directory, on which
         *  permissions are to be changed.
         * @return true if the permissions were successfully changed
         */
        boolean makeOwnerAccessOnly(File f)
            throws IOException;

        /**
         * Given an abstract file, attempt to change permissions so that it
         * is writable only by the owner of the file.
         * @param f a File referencing a file or directory, on which should
         *  permissions are to be changed.
         * @return true if the permissions were successfully changed
         */
        boolean makeOwnerOnlyWriteAccess(File f)
            throws IOException;
    }

    /*
     * Implementation of Operations using Java API operations.
     * This approach fails on Windows 7.
     */
    static class JavaAPIOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            throws IOException {

            /* readable by nobody */
            boolean result = f.setReadable(false, false);

            /* writable by nobody */
            result = result && f.setWritable(false, false);

            /* add back readability for owner */
            result = result && f.setReadable(true, true);

            /* add back writability for owner */
            result = result && f.setWritable(true, true);

            return result;
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            /*throws IOException*/ {

            /* writable by nobody */
            final boolean result = f.setWritable(false, false);

            /* add back writability for owner */
            return f.setWritable(true, true) && result;
        }
    }

    /*
     * Change access permissions using *nix shell tools
     */
    static class XNixCmdLineOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            throws IOException {

            final int oChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "o-rwx", f.getPath() });
            final int gChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "g-rwx", f.getPath() });

            return (oChmodResult == 0 && gChmodResult == 0);
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            throws IOException {

            final int oChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "o-w", f.getPath() });
            final int gChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "g-w", f.getPath() });

            return (oChmodResult == 0 && gChmodResult == 0);
        }
    }

    static class WindowsCmdLineOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            throws IOException {

            throw new UnsupportedOperationException(
                "operation not supported on the windows platform");
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            /*throws IOException*/ {

            throw new UnsupportedOperationException(
                "operation not supported on the windows platform");
        }
    }
}
