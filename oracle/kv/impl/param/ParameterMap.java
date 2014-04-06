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

package oracle.kv.impl.param;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import oracle.kv.impl.param.ParameterState;

import com.sleepycat.persist.model.Persistent;

/**
 *
 *  Version number for catalog and map to match.
 *  Map vs table.
 *  Deal with enums...
 *
 * Questions:
 *   o better validation -- abstract method?  if so, how/when to set.
 *   o standalone package, no external imports
 *   o what is persisted?
 *
 *   o use a test case to validate the enum strings
 */
@Persistent(version=1)
public class ParameterMap implements Serializable {
    private static final long serialVersionUID = 1L;
    private final static String eol = System.getProperty("line.separator");
    private static final String INDENT = "  ";
    private static final String PROPINDENT = "    ";

    private String name;
    private String type;
    private boolean validate;
    private final int version;
    private final HashMap<String, Parameter> parameters;

    private static final Parameter nullParameter = new NullParameter();

    public ParameterMap() {
        this(null, null, true, ParameterState.PARAMETER_VERSION);
    }

    public ParameterMap(String name, String type) {
        this(name, type, true, ParameterState.PARAMETER_VERSION);
    }

    public ParameterMap(String name,
                        String type,
                        boolean validate,
                        int version) {
        parameters = new HashMap<String,Parameter>();
        this.name = name;
        this.type = type;
        this.validate = validate;
        this.version = version;
    }

    public Collection<Parameter> values() {
        return parameters.values();
    }

    public Set<Map.Entry<String, Parameter>> entrySet() {
        return parameters.entrySet();
    }

    public Set<String> keys() {
        return parameters.keySet();
    }

    public int size() {
        return parameters.size();
    }

    public int getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValidate(boolean value) {
        this.validate = value;
    }

    public ParameterMap copy() {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);
        newParams.parameters.putAll(parameters);
        return newParams;
    }

    public ParameterMap filter(EnumSet<ParameterState.Info> set) {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null && pstate.containsAll(set)) {
                newParams.put(p);
            }
        }
        return newParams;
    }

    /**
     * Create a new map, filtering parameters based on set:
     *  if (positive) and name is in the set, include it
     *  if (!positive) and name is not in the set, include it
     * Skip parameters entirely if they cannot be found in
     * ParameterState.
     */
    public ParameterMap filter(Set<String> set, boolean positive) {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : values()) {
            /* skip unknown parameters */
            if (ParameterState.lookup(p.getName()) != null) {
                if (set.contains(p.getName())) {
                    if (positive) {
                        newParams.put(p);
                    }
                } else if (!positive) {
                    newParams.put(p);
                }
            }
        }
        return newParams;
    }

    /**
     * Filter out read-only parameters.  Ignore unrecognized parameters.
     */
    public ParameterMap readOnlyFilter() {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null && !pstate.getReadOnly()) {
                newParams.put(p);
            }
        }
        return newParams;
    }

    /**
     * Filter based on Scope.  Ignore unrecognized parameters.
     */
    public ParameterMap filter(ParameterState.Scope scope) {
        ParameterMap newParams =
            new ParameterMap(name, type, validate, version);

        for (Parameter p : values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null && pstate.getScope() == scope) {
                newParams.put(p);
            }
        }
        return newParams;
    }
    
    /**
     * Filter based on Info
     * @param includeParamWithInfo if true, include params that have "info"
     * attribute. If false, include params that don't have "info" attribute.
     */
    public ParameterMap filter(ParameterState.Info info, 
                               boolean includeParamWithInfo) {
        ParameterMap newParams =
                new ParameterMap(name, type, validate, version);

        for (Parameter p : values()) {
            ParameterState pstate = ParameterState.lookup(p.getName());
            if (pstate != null) {
                if ((includeParamWithInfo && pstate.appliesTo(info)) ||
                    (!includeParamWithInfo && !pstate.appliesTo(info))) {
                     newParams.put(p);
                }
            }
        }
        return newParams;
    }

    public Parameter put(Parameter value) {
        return parameters.put(value.getName(), value);
    }

    public Parameter get(String pname) {
        Parameter p = parameters.get(pname);
        if (p != null) {
            return p;
        }
        /* TODO: should this be a static? */
        return nullParameter;
    }

    /**
     * Returns the parameter value if it exists, otherwise returns its default
     * value as specified in ParameterState.
     */
    public Parameter getOrDefault(String pname) {

        if (exists(pname)) {
            return get(pname);
        }
        return DefaultParameter.getDefaultParameter(pname);
    }

    public Parameter remove(String pname) {
        return parameters.remove(pname);
    }

    /**
     * These methods allow a caller to not worry about checking null
     */
    public int getOrZeroInt(String pname) {
        Parameter p = parameters.get(pname);
        if (p != null) {
            return p.asInt();
        }
        return 0;
    }

    public long getOrZeroLong(String pname) {
        Parameter p = parameters.get(pname);
        if (p != null) {
            return p.asLong();
        }
        return 0;
    }

    public boolean exists(String pname) {
        Parameter p = parameters.get(pname);
        return (p != null);
    }

    public void clear() {
        parameters.clear();
    }

    public boolean equals(ParameterMap other) {
        if (size() != other.size()) {
            return false;
        }
        for (Parameter p : parameters.values()) {
            if (!p.equals(other.get(p.getName()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Merge other's values into this map.
     * @param notReadOnly do not merge read-only parameters
     * @return the number of values merged.
     */
    public int merge(ParameterMap other, boolean notReadOnly) {
        int numMerged = 0;
        for (Parameter p : other.values()) {
            if (!get(p.getName()).equals(p)) {
                if (notReadOnly) {
                    ParameterState pstate =
                        ParameterState.lookup(p.getName());
                    if (pstate.getReadOnly()) {
                        continue;
                    }
                }
                put(p);
                ++numMerged;
            }
        }
        return numMerged;
    }

    public boolean hasRestartRequiredDiff(ParameterMap other) {
        for (Parameter p : parameters.values()) {
            if (!p.equals(other.get(p.getName()))) {
                if (p.restartRequired()) {
                    return true;
                }
            }
        }
        return false;
    }

    /** One or more parameters in this map is a restart required param. */
    public boolean hasRestartRequired() {
        for (Parameter p : parameters.values()) {
           if (p.restartRequired()) {
               return true;
            }
        }
        return false;
    }

    /**
     * Return a map that has the values from "other" that are different
     * from "this"
     */
    public ParameterMap diff(ParameterMap other, boolean notReadOnly) {
        ParameterMap map = new ParameterMap();
        for (Parameter p : other.values()) {
            if (!get(p.getName()).equals(p)) {
                if (notReadOnly) {
                    ParameterState pstate =
                        ParameterState.lookup(p.getName());
                    if (pstate.getReadOnly()) {
                        continue;
                    }
                }
                map.put(p);
            }
        }
        return map;
    }

    public void write(PrintWriter writer) {
        writer.printf("%s<component name=\"%s\" type=\"%s\" validate=\"%s\">\n",
                      INDENT, name, type, Boolean.toString(validate));
        for (Parameter p : parameters.values()) {
            writer.printf
                ("%s<property name=\"%s\" value=\"%s\" type=\"%s\"/>\n",
                 PROPINDENT, p.getName(), p.asString(),
                 p.getType().toString());
        }
        writer.printf("%s</component>\n", INDENT);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (name != null) {
            sb.append("name=" + name);
        }
        if (type != null) {
            sb.append(" type=" + type);
        }
        sb.append(parameters.toString());
        return sb.toString();
    }

    public String showContents() {
        return showContents(false);
    }

    public String showContents(boolean sort) {
        if (!sort) {
            return parameters.toString();
        }
        StringBuilder sb = new StringBuilder();
        TreeSet<String> set = new TreeSet<String>(keys());
        boolean first = true;
        for (String s : set) {
            Parameter p = get(s);
            if (first) {
                first = false;
            } else {
                sb.append(eol);
            }
            sb.append(p.getName()).append("=").append(p);
        }
        return sb.toString();
    }

    /**
     * Set or create parameter in map.  If the value is null this means remove
     * the parameter.  If the map is set to not validate treat all parameters
     * as STRING.
     */
    public boolean setParameter(String name,
                                String value) {
        return setParameter(name, value, false);
    }

    /**
     * Allow caller to ignore unknown parameters.  This allows the system to
     * silently ignore unrecognized (probably removed) parameters.
     */
    public boolean setParameter(String name,
                                String value,
                                boolean ignoreUnknown) {
        if (value == null) {
            return ((remove(name)) != null);
        }
        Parameter p = null;
        if (validate) {
            p = Parameter.createParameter(name, value, ignoreUnknown);
        } else {
            p = Parameter.createParameter(name, value,
                                          ParameterState.Type.STRING);
        }
        if (p != null) {
            put(p);
            return true;
        }
        return false;
    }

    /**
     * Create a ParameterMap of default Parameter objects for all POLICY
     * parameters.
     */
    public static ParameterMap createDefaultPolicyMap() {
        ParameterMap map = new ParameterMap();
        for (ParameterState ps : ParameterState.pstate.values()) {
            if (ps.getPolicyState()) {
                map.put(DefaultParameter.getDefaultParameter(ps));
            }
        }
        return map;
    }

    /**
     * Add default Parameter objects for parameters that are associated with
     * the service and are POLICY parameters.  These are the only parameters
     * that are suitable for defaulting.  Do not overwrite unless overwrite is
     * true
     */
    public void addServicePolicyDefaults(ParameterState.Info service) {
        EnumSet<ParameterState.Info> set =
            EnumSet.of(ParameterState.Info.POLICY,service);
        for (ParameterState ps : ParameterState.getMap().values()) {
            if (ps.containsAll(set)) {
                final String defaultName =
                    DefaultParameter.getDefaultParameter(ps).getName();
                /*
                 * Do not overwrite existing keys
                 */
                if (!exists(defaultName)) {
                    put(DefaultParameter.getDefaultParameter(ps));
                }
            }
        }
    }

    /**
     * NullParameter exists to avoid extra checks for null objects.  It
     * should only ever happen for StringParameter.  It'll throw otherwise.
     */
    private static class NullParameter extends Parameter {
    	private static final long serialVersionUID = 1L;

        public NullParameter() {
            super("noname");
        }

        @Override
        public String asString() {
            return null;
        }

        @Override
        public ParameterState.Type getType() {
            return ParameterState.Type.NONE;
        }
    }
}
