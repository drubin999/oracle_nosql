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

package oracle.kv;

import java.io.InputStream;
import java.io.IOException;
import java.io.Serializable;

import java.util.Properties;

/**
 * Oracle NoSQL DB version information.  Versions consist of major, minor and
 * patch numbers.
 *
 * There is one KVVersion object per running JVM and it may be accessed using
 * the static field KVVersion.CURRENT_VERSION.
 */
public class KVVersion implements Comparable<KVVersion>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Release versions.
     */
    public static final KVVersion R1_2_123 =
        new KVVersion(11, 2, 1, 2, 123, null);  /* R1.2.123 */
    public static final KVVersion R2_0_23 =
        new KVVersion(11, 2, 2, 0, 23, null);   /* R2.0 10/2012 */
    public static final KVVersion R2_1_0 =
        new KVVersion(12, 1, 2, 1, 0, null);    /* R2.1 Q2/2013 */
    public static final KVVersion R3_0 =
        new KVVersion(12, 1, 3, 0, 5, null);    /* R3.0 Q1/2014 */

    public static final KVVersion CURRENT_VERSION =
        /*
         * WHEN YOU BUMP THIS VERSION, BE SURE TO BUMP THE VERSIONS IN
         * misc/rpm/*.spec.
         */
        R3_0;

   /**
     * Prerequisite version.
     */
    public static final KVVersion PREREQUISITE_VERSION = R2_0_23;

    private final int oracleMajor;
    private final int oracleMinor;
    private final int majorNum;
    private final int minorNum;
    private final int patchNum;
    private String releaseId = null;
    private String releaseDate = null;
    private final String name;
    private Properties versionProps;

    public static void main(String argv[]) {
        System.out.println(CURRENT_VERSION);
    }

    public KVVersion(int oracleMajor,
                     int oracleMinor,
                     int majorNum,
                     int minorNum,
                     int patchNum,
                     String name) {
        this.oracleMajor = oracleMajor;
        this.oracleMinor = oracleMinor;
        this.majorNum = majorNum;
        this.minorNum = minorNum;
        this.patchNum = patchNum;
        this.name = name;
    }

    @Override
    public String toString() {
        return getVersionString();
    }

    /**
     * Oracle Major number of the release version.
     *
     * @return The Oracle major number of the release version.
     */
    public int getOracleMajor() {
        return oracleMajor;
    }

    /**
     * Oracle Minor number of the release version.
     *
     * @return The Oracle minor number of the release version.
     */
    public int getOracleMinor() {
        return oracleMinor;
    }

    /**
     * Major number of the release version.
     *
     * @return The major number of the release version.
     */
    public int getMajor() {
        return majorNum;
    }

    /**
     * Minor number of the release version.
     *
     * @return The minor number of the release version.
     */
    public int getMinor() {
        return minorNum;
    }

    /**
     * Patch number of the release version.
     *
     * @return The patch number of the release version.
     */
    public int getPatch() {
        return patchNum;
    }

    public String getReleaseId() {
        initVersionProps();
        return releaseId;
    }

    public String getReleaseDate() {
        initVersionProps();
        return releaseDate;
    }

    private synchronized void initVersionProps() {
        if (versionProps != null) {
            return;
        }

        final InputStream releaseProps =
            KVVersion.class.getResourceAsStream("/version/build.properties");
        if (releaseProps == null) {
            return;
        }

        versionProps = new Properties();
        try {
            versionProps.load(releaseProps);
        } catch (IOException IOE) {
            throw new IllegalStateException(IOE);
        }
        releaseId = versionProps.getProperty("release.id");
        releaseDate = versionProps.getProperty("release.date");
    }

    /**
     * The numeric version string, without the patch tag.
     *
     * @return The release version
     */
    public String getNumericVersionString() {
        StringBuilder version = new StringBuilder();
        version.append(oracleMajor).append(".");
        version.append(oracleMinor).append(".");
        version.append(majorNum).append(".");
        version.append(minorNum).append(".");
        version.append(patchNum);
        return version.toString();
    }

    /**
     * Release version, suitable for display.
     *
     * @return The release version, suitable for display.
     */
    public String getVersionString() {
        initVersionProps();
        StringBuilder version = new StringBuilder();
        version.append(oracleMajor);
        version.append((oracleMajor == 12 ? "cR" : "gR"));
        version.append(oracleMinor).append(".");
        version.append(majorNum).append(".");
        version.append(minorNum).append(".");
        version.append(patchNum);
        if (name != null) {
            version.append(" (");
            version.append(name);
            version.append(")");
        }
        if (releaseId != null) {
            version.append(" ").append(releaseDate).append(" ");
            version.append(" Build id: ").append(releaseId);
        }
        return version.toString();
    }

    /**
     * Returns a KVVersion object representing the specified version string
     * without the release ID, release date,and name parts filled in. This
     * method is basically the inverse of getNumericVersionString(). This
     * method will also parse a full version string (returned from toString())
     * but only the numeric version portion of the string.
     *
     * @param versionString version string to parse
     * @return a KVVersion object
     */
    public static KVVersion parseVersion(String versionString) {

        /*
         * The full verion string will have spaces after the numeric portion of
         * version.
         */
        final String[] tokens = versionString.split(" ");

        /*
         * The full version will have "cR" or "gR" in the numeric portion of the
         * string. So we convert it to a numeric version (replace
         * the "cR"/"gR" with ".").
         */
        final String numericString = tokens[0].replaceAll("[cg]R", ".");

        final String[] numericTokens = numericString.split("\\.");

        if (numericTokens.length != 5) {
            throw new IllegalArgumentException
                ("Invalid version string: " + versionString);
        }

        try {
            return new KVVersion(Integer.parseInt(numericTokens[0]),
                                 Integer.parseInt(numericTokens[1]),
                                 Integer.parseInt(numericTokens[2]),
                                 Integer.parseInt(numericTokens[3]),
                                 Integer.parseInt(numericTokens[4]),
                                 null);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
                ("Invalid version string: " + versionString, nfe);
        }
    }

    /*
     * Return -1 if the current version is earlier than the comparedVersion.
     * Return 0 if the current version is the same as the comparedVersion.
     * Return 1 if the current version is later than the comparedVersion.
     */
    @Override
    public int compareTo(KVVersion comparedVersion) {
        int result = 0;

        if (oracleMajor == comparedVersion.getOracleMajor()) {
            if (oracleMinor == comparedVersion.getOracleMinor()) {
                if (majorNum == comparedVersion.getMajor()) {
                    if (minorNum == comparedVersion.getMinor()) {
                        if (patchNum > comparedVersion.getPatch()) {
                            result = 1;
                        } else if (patchNum < comparedVersion.getPatch()) {
                            result = -1;
                        }
                    } else if (minorNum > comparedVersion.getMinor()) {
                        result = 1;
                    } else {
                        result = -1;
                    }
                } else if (majorNum > comparedVersion.getMajor()) {
                    result = 1;
                } else {
                    result = -1;
                }
            } else if (oracleMinor > comparedVersion.getOracleMinor()) {
                result = 1;
            } else {
                result = -1;
            }
        } else if (oracleMajor > comparedVersion.getOracleMajor()) {
            result = 1;
        } else {
            result = -1;
        }

        return result;
    }

    /*
     * If its type is KVVersion, and the version numbers are the same,
     * then we consider these two versions equal.
     */
    @Override
    public boolean equals(Object o) {
        return (o instanceof KVVersion) && (compareTo((KVVersion) o) == 0);
    }

    /* Produce a unique hash code for KVVersion. */
    @Override
    public int hashCode() {
        return majorNum * 1000 * 1000 + minorNum * 1000 + patchNum;
    }
}
