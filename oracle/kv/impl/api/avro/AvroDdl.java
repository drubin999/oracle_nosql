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

package oracle.kv.impl.api.avro;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import oracle.kv.Consistency;
import oracle.kv.KVStore;
import oracle.kv.KVVersion;
import oracle.kv.impl.admin.AdminService;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.NonfatalAssertionException;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.util.Pair;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

/**
 * Implements Admin DDL schema commands.  Called by admin service.
 */
public class AvroDdl {
    private final static String eol = System.getProperty("line.separator");

    /**
     * Holds the summary information for a schema.
     *
     * This serializable class is used in the admin CommandService RMI
     * interface.  Fields may be added without adding new remote methods.
     */
    static class BaseSchemaSummary implements Serializable {

        private static final long serialVersionUID = 1L;

        private final AvroSchemaMetadata metadata;
        private final String name;
        private final int id;

        BaseSchemaSummary(AvroSchemaMetadata metadata, String name, int id) {
            this.metadata = metadata;
            this.name = name;
            this.id = id;
        }
        
        public AvroSchemaMetadata getMetadata() {
            return metadata;
        }
        
        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }
    }

    /**
     * Holds the summary information for a schema plus a link to the previous
     * version.
     *
     * This serializable class is used in the admin CommandService RMI
     * interface.  Fields may be added without adding new remote methods.
     */
    public static class SchemaSummary extends BaseSchemaSummary {

        private static final long serialVersionUID = 1L;

        private final SchemaSummary prevVersion;

        SchemaSummary(AvroSchemaMetadata metadata,
                      String name,
                      int id,
                      SchemaSummary prevVersion) {
            super(metadata, name, id);
            this.prevVersion = prevVersion;
        }

        public SchemaSummary getPreviousVersion() {
            return prevVersion;
        }
    }

    /**
     * Holds the summary information and full text for a schema.
     *
     * This serializable class is used in the admin CommandService RMI
     * interface.  Fields may be added without adding new remote methods.
     */
    public static class SchemaDetails extends BaseSchemaSummary {

        private static final long serialVersionUID = 1L;

        private final String text;

        SchemaDetails(AvroSchemaMetadata metadata,
                      String name,
                      int id,
                      String text) {
            super(metadata, name, id);
            this.text = text;
        }
        
        public String getText() {
            return text;
        }
    }

    public static class AddSchemaOptions implements Serializable {

        private static final long serialVersionUID = 1L;

        private final boolean evolve;
        private final boolean force;

        public AddSchemaOptions(boolean evolve, boolean force) {
            this.evolve = evolve;
            this.force = force;
        }

        public boolean getEvolve() {
            return evolve;
        }

        public boolean getForce() {
            return force;
        }
    }

    public static class AddSchemaResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int id;
        private final String extraMsg;

        AddSchemaResult(int id, String extraMsg) {
            this.id = id;
            this.extraMsg = extraMsg;
        }

        public int getId() {
            return id;
        }

        public String getExtraMessage() {
            return extraMsg;
        }
    }

    /**
     * Implemented for the command-specific portion of an Avro DDL command.
     * Called by runSchemaCommand, which also implements the common logic for
     * all commands.
     */
    public interface Command<T> {
        T execute(AvroDdl ddl);
    }

    /**
     * Runs a schema command using the standard admin fault handler.
     *
     * Opens the KVStore on behalf of the command, and ensures it is closed.
     * <p>
     * Provides special handling for exceptions, namely that a runtime
     * exception (that is not IllegalCommandException or
     * NonfatalAssertionException) is wrapped in a NonfatalAssertionException.
     * The idea is that no schema command should cause a retry or an admin
     * process restart, as would be done if OperationFaultException or a plain
     * runtime exception were thrown to the handler. Since schema ddl commands
     * don't access the admin JE environment and are all stateless, they are
     * not impacted by admin resource availability or bad state.
     */
    public static <T> T execute(final AdminService aservice,
                                final Command<T> cmd) {

        return aservice.getFaultHandler().execute
            (new ProcessFaultHandler.SimpleOperation<T>() {

            @Override
            public T execute() {
                final KVStore store = aservice.getAdmin().openKVStore();
                boolean noException = false;
                try {
                    final T result = cmd.execute(new AvroDdl(store));
                    noException = true;
                    return result;
                } catch (IllegalCommandException e) {
                    throw e;
                } catch (NonfatalAssertionException e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw new NonfatalAssertionException
                        ("Unexpected exception during schema command: " +
                         e.toString(), e);
                } finally {
                    try {
                        store.close();
                    } catch (RuntimeException e) {
                        /* Ignore this if another exception is in flight. */
                        if (noException) {
                            throw new NonfatalAssertionException
                                ("Exception closing KVStore after successful" +
                                 " schema command", e);
                        }
                    }
                }
            }
        });
    }

    private final SchemaAccessor accessor;

    public AvroDdl(KVStore store) {
        this.accessor = new SchemaAccessor(store);
    }

    public SortedMap<String, SchemaSummary>
        getSchemaSummaries(boolean includeDisabled) {

        final SortedMap<String, SchemaSummary> results =
            new TreeMap<String, SchemaSummary>();

        final SortedMap<Integer, SchemaData> schemas =
            accessor.readAllSchemas(includeDisabled, Consistency.ABSOLUTE);

        for (SortedMap.Entry<Integer, SchemaData> entry : schemas.entrySet()) {
            final Integer id = entry.getKey();
            final SchemaData data = entry.getValue();
            final String name = data.getSchema().getFullName();
            final SchemaSummary prevVersion = results.get(name);
            final SchemaSummary summary =
                new SchemaSummary(data.getMetadata(), name, id, prevVersion);
            results.put(name, summary);
        }

        return results;
    }

    public SchemaDetails getSchemaDetails(final int schemaId) {

        final SchemaData data;
        try {
            data = accessor.readSchema(schemaId, Consistency.ABSOLUTE);
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandException(e.getMessage(), e);
        }

        final Schema schema = data.getSchema();
        final String text = schema.toString(true /*pretty*/);

        return new SchemaDetails
            (data.getMetadata(), schema.getFullName(), schemaId, text);
    }

    public boolean updateSchemaStatus(int schemaId,
                                      AvroSchemaMetadata metadata,
                                      KVVersion version) {
        try {
            return accessor.updateSchemaStatus(schemaId, metadata, version);
        } catch (IllegalArgumentException e) {
            throw new IllegalCommandException(e.getMessage(), e);
        }
    }

    public AddSchemaResult addSchema(AvroSchemaMetadata metadata,
                                     String schemaText,
                                     AddSchemaOptions options,
                                     KVVersion version) {
        final Schema newSchema;
        try {
            newSchema = new Schema.Parser().parse(schemaText);
        } catch (SchemaParseException e) {
            throw new IllegalCommandException(e.getMessage(), e);
        }

        final String name = newSchema.getFullName();
        if (newSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalCommandException
                ("Top level schema is not a 'record': " + name);
        }

        final SortedMap<Integer, SchemaData> allSchemas =
            accessor.readAllSchemas(false /*includeDisabled*/,
                                    Consistency.ABSOLUTE);

        /* Collect other versions of this schema. */
        final SortedMap<Integer, Schema> oldSchemas =
            new TreeMap<Integer, Schema>();
        for (final SortedMap.Entry<Integer, SchemaData> entry :
             allSchemas.entrySet()) {
            final Schema schema = entry.getValue().getSchema();
            if (name.equals(schema.getFullName())) {
                oldSchemas.put(entry.getKey(), schema);
            }
        }

        /* Check for legal add or change. */
        if (options.getEvolve()) {
            if (oldSchemas.size() == 0) {
                throw new IllegalCommandException
                    ("Cannot change schema, does not exist: " + name);
            }
        } else {
            if (oldSchemas.size() != 0) {
                throw new IllegalCommandException
                    ("Cannot add schema, already exists: " + name);
            }
        }

        /*
         * Check for evolution errors and warnings [#21691].  First check for
         * problems that apply to the new schema alone.
         */
        int nErrors = 0;
        int nWarnings = 0;
        List<String> errors = new ArrayList<String>();
        List<String> warnings = new ArrayList<String>();
        SchemaChecker.checkSchema(newSchema, errors, warnings);
        nErrors += errors.size();
        nWarnings += warnings.size();
        final Pair<List<String>, List<String>> newSchemaErrorsAndWarnings =
            new Pair<List<String>, List<String>>(errors, warnings);

        /* Check for problems regarding evolution from an older schema. */
        final SortedMap<Integer, Pair<List<String>, List<String>>>
            evolutionErrorsAndWarnings =
            new TreeMap<Integer, Pair<List<String>, List<String>>>();
        for (final SortedMap.Entry<Integer, Schema> entry :
             oldSchemas.entrySet()) {
            final Schema oldSchema = entry.getValue();
            errors = new ArrayList<String>();
            warnings = new ArrayList<String>();
            SchemaChecker.checkEvolution(oldSchema, newSchema, errors,
                                         warnings);
            nErrors += errors.size();
            nWarnings += warnings.size();
            if (errors.size() > 0 || warnings.size() > 0) {
                final Integer oldVersion = entry.getKey();
                evolutionErrorsAndWarnings.put
                    (oldVersion,
                     new Pair<List<String>, List<String>>(errors, warnings));
            }
        }

        /* If there are any problems, we may need to abort the command. */
        if (nErrors > 0 || (nWarnings > 0 && !options.getForce())) {
            final String msg = formatErrorsAndWarnings
                (newSchema.getFullName(), nErrors, nWarnings,
                 newSchemaErrorsAndWarnings, evolutionErrorsAndWarnings);
            throw new IllegalCommandException(msg);
        }

        /* Insert the schema. */
        final SchemaData data = new SchemaData(metadata, newSchema);
        final int newId = accessor.insertSchema(data, version);

        /* We've decided to proceed in spite of any problems. */
        final StringBuilder extraMsg = new StringBuilder(100);
        if (nErrors > 0 || nWarnings > 0) {
            appendErrorsAndWarningsSummary(extraMsg, nErrors, nWarnings,
                                           "was", "were");
            extraMsg.append(" ignored.");
        }

        return new AddSchemaResult(newId, extraMsg.toString());
    }

    private String formatErrorsAndWarnings
        (String schemaName,
         int nTotalErrors,
         int nTotalWarnings,
         Pair<List<String>, List<String>> newSchemaErrorsAndWarnings,
         SortedMap<Integer, Pair<List<String>, List<String>>>
         evolutionErrorsAndWarnings) {

        final StringBuilder b = new StringBuilder(1000);

        b.append("Schema was not added because ");
        appendErrorsAndWarningsSummary(b, nTotalErrors, nTotalWarnings,
                                       "was", "were");
        b.append(" detected.");
        b.append(eol);
        if (nTotalWarnings > 0) {
            b.append("To override warnings, specify -force.");
        }
        if (nTotalErrors > 0) {
            if (nTotalWarnings > 0) {
                b.append(' ');
            }
            b.append("Errors cannot be overridden with -force.");
        }
        b.append(eol);

        if (newSchemaErrorsAndWarnings.first().size() > 0 ||
            newSchemaErrorsAndWarnings.second().size() > 0) {
            b.append(eol);
            b.append("The following ");
            appendErrorsAndWarningsSummary
                (b, newSchemaErrorsAndWarnings.first().size(),
                 newSchemaErrorsAndWarnings.second().size(),
                 "applies", "apply");
            b.append(" to the new schema being added.");
            b.append(eol).append(eol);
            appendErrorsAndWarnings(b, newSchemaErrorsAndWarnings.first(),
                                    newSchemaErrorsAndWarnings.second());
        }

        for (final SortedMap.Entry<Integer, Pair<List<String>, List<String>>>
             entry : evolutionErrorsAndWarnings.entrySet()) {
            final String oldName = schemaName + "." + entry.getKey();
            final Pair<List<String>, List<String>> errorsAndWarnings =
                entry.getValue();
            b.append(eol);
            b.append("The following ");
            appendErrorsAndWarningsSummary
                (b, errorsAndWarnings.first().size(),
                 errorsAndWarnings.second().size(),
                 "applies", "apply");
            b.append(" to evolution from ");
            b.append(eol);
            b.append(oldName).append(" to the schema being added.");
            b.append(eol).append(eol);
            appendErrorsAndWarnings(b, errorsAndWarnings.first(),
                                    errorsAndWarnings.second());
        }

        return b.toString();
    }

    private void appendErrorsAndWarningsSummary(StringBuilder b,
                                                int nErrors,
                                                int nWarnings,
                                                String singularSuffix,
                                                String pluralSuffix) {
        if (nErrors > 0) {
            b.append(nErrors).append(" error");
            if (nErrors > 1) {
                b.append('s');
            }
            if (nWarnings > 0) {
                b.append(" and ");
            }
        }
        if (nWarnings > 0) {
            b.append(nWarnings).append(" warning");
            if (nWarnings > 1) {
                b.append('s');
            }
        }
        b.append(' ');
        if (nErrors > 1 || nWarnings > 1 ||
            (nErrors > 0 && nWarnings > 0)) {
            b.append(pluralSuffix);
        } else {
            b.append(singularSuffix);
        }
    }

    private void appendErrorsAndWarnings(StringBuilder b,
                                         List<String> errors,
                                         List<String> warnings) {
        for (final String error : errors) {
            b.append("ERROR: ").append(error).append(eol);
        }
        for (final String warning : warnings) {
            b.append("WARNING: ").append(warning).append(eol);
        }
    }

    public void deleteAllSchemas() {
        accessor.deleteAllSchemas();
    }
}
