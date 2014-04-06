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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;

import org.codehaus.jackson.JsonNode;

/**
 * Utilities for comparing schemas for equality, and for checking and comparing
 * schemas to find evolution problems and other schema validity problems.
 */
class SchemaChecker {

    /**
     * Determines whether two schemas are equal with respect to their behavior
     * in performing data serialization and deserialization.  Attributes that
     * do not impact serialization are ignored by the comparison.  If
     * allowNullDefault is true a null default value will match a non-null.
     * This option exists for the C api which does not currently handle
     * default values in Avro.
     * <p>
     * This comparison is different than Schema.equals in that properties are
     * not compared here, but are compared by Schema.equals.  Ignoring the
     * string type property is particularly important here, because it is
     * normally only specified in the client-side schema, and not in the stored
     * schema.
     * <p>
     * In other respects, this comparison and Schema.equals are equivalent.
     * Both this comparison and Schema.equals ignore the following attributes.
     *  + doc attribute
     *  + order attribute
     *  + field and type aliases
     */

    static boolean equalSerialization(Schema s1, Schema s2) {
        return schemasEqualWithDefault
            (s1, s2, new HashSet<SchemaPair>(100), false);
    }

    static boolean equalSerializationWithDefault(Schema s1, Schema s2,
                                                 boolean allowNullDefault) {
        return schemasEqualWithDefault
            (s1, s2, new HashSet<SchemaPair>(100), allowNullDefault);
    }

    /*
     * A convenience wrapper for existing code
     */
    private static boolean schemasEqual(Schema s1,
                                        Schema s2,
                                        Set<SchemaPair> visitedSet) {
        return schemasEqualWithDefault(s1, s2, visitedSet, false);
    }

    private static boolean schemasEqualWithDefault(Schema s1,
                                                   Schema s2,
                                                   Set<SchemaPair> visitedSet,
                                                   boolean allowNullDefault) {

        /* Most efficient case first. */
        if (s1 == s2) {
            return true;
        }

        /* Prevent infinite recursion. */
        if (alreadyVisited(new SchemaPair(s1, s2), visitedSet)) {
            return true;
        }

        /* Common attributes for all schemas. */
        final Type type = s1.getType();
        if (type != s2.getType()) {
            return false;
        }

        /* Common attributes for named types (record, enum and fixed). */
        if (type == Type.RECORD || type == Type.ENUM || type == Type.FIXED) {
            final String n1 = s1.getFullName();
            final String n2 = s2.getFullName();
            if ((n1 == null) ? (n2 != null) : (!n1.equals(n2))) {
                return false;
            }
        }

        /* Use straight-line approach for last type-specific comparison. */
        switch (type) {
        case RECORD:
            final List<Field> f1 = s1.getFields();
            final List<Field> f2 = s2.getFields();

            final int nFields = f1.size();
            if (nFields != f2.size()) {
                return false;
            }

            for (int i = 0; i < nFields; i += 1) {
                if (!fieldsEqual(f1.get(i), f2.get(i),
                                 visitedSet, allowNullDefault)) {
                    return false;
                }
            }
            return true;
        case UNION:
            final List<Schema> t1 = s1.getTypes();
            final List<Schema> t2 = s2.getTypes();

            final int nTypes = t1.size();
            if (nTypes != t2.size()) {
                return false;
            }

            for (int i = 0; i < nTypes; i += 1) {
                if (!schemasEqual(t1.get(i), t2.get(i), visitedSet)) {
                    return false;
                }
            }
            return true;
        case ARRAY:
            return schemasEqual(s1.getElementType(), s2.getElementType(),
                                visitedSet);
        case MAP:
            return schemasEqual(s1.getValueType(), s2.getValueType(),
                                visitedSet);
        case ENUM:
            return s1.getEnumSymbols().equals(s2.getEnumSymbols());
        case FIXED:
            return s1.getFixedSize() == s2.getFixedSize();
        case STRING:
        case BYTES:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case NULL:
            return true;
        default:
            /* Should never happen. */
            throw new RuntimeException("Unknown type: " + type);
        }
    }

    private static boolean fieldsEqual(Field f1,
                                       Field f2,
                                       Set<SchemaPair> visitedSet,
                                       boolean allowNullDefault) {
        if (!f1.name().equals(f2.name())) {
            return false;
        }
        if (!schemasEqual(f1.schema(), f2.schema(), visitedSet)) {
            return false;
        }

        /* Use straight-line approach for last attribute, default value. */
        final JsonNode d1 = f1.defaultValue();
        final JsonNode d2 = f2.defaultValue();
        if (d1 == null || d2 == null) {
            if (d1 == null && d2 == null) {
                return true;
            } else if (allowNullDefault) {
                return (d1 == null);
            }
            return false;
        }

        /*
         * NaN values cannot be defined with JSON, so this code is unlikely to
         * ever be executed.
         */
        if (Double.isNaN(d1.getDoubleValue())) {
            return Double.isNaN(d2.getDoubleValue());
        }

        return d1.equals(d2);
    }

    /**
     * When adding a new or updated schema, checks the schema being added for
     * problems.  Warning and error messages are added to the lists passed as
     * parameters, and each message starts with "WARNING: " or "ERROR: ".
     * <p>
     * Errors are produced for the follow changes.
     * <ul>
     *  <li>
     *  A field's default value does not conform to the field's type.
     *  (Unfortunately, this is not checked by the Avro schema parser.)
     *  [#21873]
     *  <li>
     *  TODO: Oracle-defined schema types (date/time schemas) are defined
     *  incorrectly.
     * </ul>
     * <p>
     * Warnings are produced for the follow changes.
     * <ul>
     *  <li>
     *  Determines whether any of the Avro "Schema resolution" rules might fail
     *  when this schema is changed in the future. Currently, the only such
     *  check is for a default value on each field, since deleting the field in
     *  the future will only be possible if a two-phase upgrade is performed
     *  (see checkEvolution).
     * </ul>
     *
     * @param s is the new schema, the one being added.
     * @param errors list to contain errors.
     * @param warnings list to contain warnings.
     */
    static void checkSchema(Schema s,
                            List<String> errors,
                            List<String> warnings) {
        checkSchema(s, errors, warnings, new HashSet<SchemaPair>(100));
    }

    private static void checkSchema(Schema s,
                                    List<String> errors,
                                    List<String> warnings,
                                    Set<SchemaPair> visitedSet) {

        /* Prevent infinite recursion. */
        if (alreadyVisited(new SchemaPair(s, s), visitedSet)) {
            return;
        }

        switch (s.getType()) {
        case RECORD:
            for (final Field field : s.getFields()) {
                if (field.defaultValue() == null) {
                    warnings.add
                        ("Field " + makeFieldName(s, field) +
                         " does not have a default value. Without a default," +
                         " the field cannot be deleted in a future version" +
                         " of the schema. If it is deleted in the future," +
                         " data written with the new schema will not be" +
                         " readable with the old schema.");
                } else {

                    /*
                     * Check our own strict rules first so that we output a
                     * more meaningful message first.  Avro's messages are
                     * sometimes cryptic.
                     */
                    final String strictError =
                        checkDefaultValueStrictRules(field);
                    if (strictError != null) {
                        errors.add
                            ("Field " + makeFieldName(s, field) +
                             " has a default value that does not conform" +
                             " strictly to the field's type. " + strictError);
                        /* Avoid reporting duplicate errors below. */
                        continue;
                    }

                    /*
                     * For safety, also check Avro's rules. Although Avro's
                     * rules are more lenient and sometimes the messages are
                     * cryptic, they determine whether it is possible to apply
                     * the default value or not.
                     */
                    final String avroError = checkDefaultValueAvroRules(field);
                    if (avroError != null) {
                        errors.add
                            ("Field " + makeFieldName(s, field) +
                             " has a default value that does not conform to" +
                             " the field's type. " + avroError);
                        /* Avoid reporting duplicate errors below. */
                        continue;
                    }
                }

                /* Recurse on field's type. */
                checkSchema(field.schema(), errors, warnings, visitedSet);
            }
            break;
        case UNION:
            /* Recurse on each union type. */
            for (final Schema type : s.getTypes()) {
                checkSchema(type, errors, warnings, visitedSet);
            }
            break;
        case ARRAY:
            /* Recurse on array's element type. */
            checkSchema(s.getElementType(), errors, warnings, visitedSet);
            break;
        case MAP:
            /* Recurse on map's value type. */
            checkSchema(s.getValueType(), errors, warnings, visitedSet);
            break;
        case ENUM:
        case FIXED:
        case STRING:
        case BYTES:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case NULL:
            break;
        default:
            /* Should never happen. */
            throw new RuntimeException("Unknown type: " + s.getType());
        }
    }

    /**
     * Validates a field's default value against the field's schema, according
     * to Avro rules.  If these rules are violated then schema usage should not
     * be permitted, because the default value cannot be applied by Avro.  Note
     * that the Avro rules are quite lenient and are supplemented by calling
     * checkDefaultValueStrictRules.
     *
     * @return null if the value matches the type according to Avro's rules for
     * default values, or a non-null error message if it doesn't.
     */
    private static String checkDefaultValueAvroRules(Field field) {
        final Schema s = field.schema();
        final JsonNode jsonValue = field.defaultValue();

        /*
         * To ensure that Avro can process the default value, we need to be
         * sure that ResolvingGrammarGenerator.encode succeeds.  It throws a
         * runtime exception iff the value does not conform to the schema,
         * according to its (very lenient) rules.
         */
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Encoder encoder =
            EncoderFactory.get().binaryEncoder(baos, null);
        try {
            ResolvingGrammarGenerator.encode(encoder, s, jsonValue);
        } catch (RuntimeException e) {
            return "Avro default value error: " + e.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        /*
         * To further ensure that Avro can process the default value, mimic a
         * schema evolution where a field of the given type is removed, then
         * written with the new schema, and then we try reading this data using
         * the old reader schema that still contains the field.  The default
         * value will be applied and an exception will be thrown if a problem
         * occurs during serialization or deserialization.
         */
        final Schema writerSchema =
            Schema.createRecord("testRecord", "", "", false);
        writerSchema.setFields(new ArrayList<Field>());

        final Schema readerSchema =
            Schema.createRecord("testRecord", "", "", false);
        final Field testField = new Field("testField", s, "", jsonValue);
        readerSchema.setFields(Collections.singletonList(testField));

        /* Write empty record with writer schema. */
        baos.reset();
        final GenericRecord record = new GenericData.Record(writerSchema);
        /* Use our GenericDatumWriter subclass to get extra checks. */
        final GenericBinding.GenericWriter writer =
            new GenericBinding.GenericWriter(writerSchema);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (RuntimeException e) {
            return "Avro serialization error: " + e.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        /* Read with reader schema, default value is applied. */
        final GenericDatumReader<GenericRecord> reader =
            new GenericDatumReader<GenericRecord>(writerSchema, readerSchema);
        final Decoder decoder =
            DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
        try {
            reader.read(null, decoder);
        } catch (RuntimeException e) {
            return "Avro deserialization error: " + e.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        /* No Avro errors. */
        return null;
    }

    /**
     * Validates a field's default value against the field's schema, performing
     * stricter checks than those done by Avro.
     *
     * One could argue that Avro's checking of the default value (implemented
     * by checkDefaultValueAvroRules) should be sufficient, since it determines
     * whether the default value can actually be applied by Avro.  However,
     * Avro uses a special set of rules for checking default values, whether
     * during schema evolution (when a field appears in the reader schema but
     * not the writer schema) or in a builder class (when filling in an
     * uninitialized field with the default value).  In both of these cases
     * Avro ends up calling ResolvingGrammarGenerator.encode, which checks the
     * value before serializing it but is more much lenient than normal
     * serialization.
     * <p>
     * Specifically, Avro's checks are deficient as follows:
     * <ul>
     *  <li>
     *  When the field type is an array, map or record, values of the wrong
     *  JSON type (not array or object) are translated to an empty array, map
     *  or record.  This is probably the most important problem.
     *  <li>
     *  For all numeric Avro types (int, long, float and double) the default
     *  value may be of any JSON numeric type, and the JSON values will be
     *  coerced to the Avro type in spite of the fact that part of the value
     *  may be lost/truncated.
     *  <li>
     *  The byte array length is not validated for a fixed type.
     *  <li>
     *  For nested fields and certain types (e.g., enums) a cryptic error is
     *  often output that does not contain the name of the offending field.
     * </ul>
     * <p>
     * These deficiencies can mask errors made by the user when defining a
     * default value.  To compensate for these deficiencies we implement our
     * own checking that is more strict than Avro's.  To do this, we serialize
     * the default value using our own JSON serializer in a special mode where
     * default values are applied.  Any errors during serialization indicate
     * that the default value is invalid.
     */
    private static String checkDefaultValueStrictRules(Field field) {
        final Schema s = field.schema();
        final JsonNode jsonValue = field.defaultValue();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final JsonBinding.JsonDatumWriter writer =
            new JsonBinding.JsonDatumWriter(s, true /*applyDefaultValues*/);
        final Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

        try {
            writer.write(s, jsonValue, encoder);
            encoder.flush();
        } catch (RuntimeException e) {
            return e.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    /**
     * When updating a schema from s1 to s2, checks whether any of the Avro
     * "Schema resolution" rules might fail [#21691].  Warning and error
     * messages are added to the lists passed as parameters, and each message
     * starts with "WARNING: " or "ERROR: ".
     * <p>
     * Errors are produced for the follow changes.  These are considered fatal
     * because data written with the old schema will not be readable with the
     * new schema.
     * <ul>
     *  <li>
     *  A field is added without a default value.
     *  <p>
     *  The size of a fixed type is changed.
     *  <li>
     *  An enum symbol is removed.
     *  <li>
     *  A union type is removed or, equivalently, a union type is changed to a
     *  non-union type and the new type is not the sole type in the old union.
     *  <li>
     *  A change to a field's type (specifically to a different type name) is
     *  considered an error except when it is a type promotion, as defined by
     *  the Avro spec. And even a type promotion is a warning; see below.
     *  Another exception is changing from a non-union to a union; see below.
     *  <li>
     * </ul>
     * <p>
     * The warnings produced are as follows.  These are considered non-fatal
     * only because a two-phase upgrade can be used to avoid problems.  In a
     * two-phase upgrade, all clients begin using the schema only for reading
     * in phase I (the old schema is still used for writing), and then use the
     * new schema for both reading and writing in phase II.  Phase II may not
     * be begun until phase I is complete, i.e., no client may use the new
     * schema for writing until all clients are using it for reading.
     * <ul>
     *  <li>
     *  A field is deleted in the new schema when it does not contain a default
     *  value in the old schema.  This warning is in addition to the warning
     *  added by checkSchema.
     *  <li>
     *  An enum symbol is added.
     *  <li>
     *  A union type is added or, equivalently, a non-union type is changed to
     *  a union that includes the original type and additional types.
     *  <li>
     *  A field's type is promoted, as defined by the Avro spec.  Type
     *  promotions are: int to long, float or double; long to float or double;
     *  float to double.
     * </ul>
     * <p>
     * This leaves the following changes that can be made without an error or a
     * warning.  In other words, these changes are safe even when a two-phase
     * upgrade is not used.
     * <ul>
     *  <li>
     *  A field with a default value is added.
     *  <li>
     *  A field that was previously defined with a default value is removed.
     *  <li>
     *  A field's doc attribute is changed, added or removed.
     *  <li>
     *  A field's order attribute is changed, added or removed.
     *  <li>
     *  A field's default value is added or changed.
     *  <li>
     *  Field or type aliases are added or removed.
     *  <li>
     *  A non-union type may be changed to a union that contains only the
     *  original type, or vice-versa.
     * </ul>
     *
     * @param s1 is the previous version of the schema.
     * @param s2 is the new version of the schema, the one being added.
     * @param errors list to contain evolution errors.
     * @param warnings list to contain evolution warnings.
     */
    static void checkEvolution(Schema s1,
                               Schema s2,
                               List<String> errors,
                               List<String> warnings) {
        checkRecordEvolution(s1, s2, errors, warnings,
                             new HashSet<SchemaPair>(100));
    }

    private static final String CANNOT_READ_WITH_OLD_SCHEMA =
         " Data written with the new schema will" +
         " not be readable with the old schema.";
    private static final String CANNOT_READ_WITH_NEW_SCHEMA =
         " Data written with the old schema will" +
         " not be readable with the new schema.";
    private static final String CANNOT_READ_WITH_EITHER_SCHEMA =
         " Data written with one schema will" +
         " not be readable with the other schema.";

    private static void checkRecordEvolution(Schema s1,
                                             Schema s2,
                                             List<String> errors,
                                             List<String> warnings,
                                             Set<SchemaPair> visitedSet) {

        for (final Field f1 : s1.getFields()) {
            if (s2.getField(f1.name()) == null) {
                if (f1.defaultValue() == null) {
                    warnings.add
                        ("Field " + makeFieldName(s1, f1) +
                         " was deleted but does not have a default" +
                         " value." + CANNOT_READ_WITH_OLD_SCHEMA);
                }
            }
        }
        for (final Field f2 : s2.getFields()) {
            final Field f1 = s1.getField(f2.name());
            if (f1 == null) {
                if (f2.defaultValue() == null) {
                    errors.add
                        ("Field " + makeFieldName(s2, f2) +
                         " was added but does not have a default" +
                         " value." + CANNOT_READ_WITH_NEW_SCHEMA);
                }
            } else {
                checkTypeEvolution
                    (f1.schema(), f2.schema(),
                     "The type of field " + makeFieldName(s2, f2) +
                     " was changed.",
                     errors, warnings, visitedSet);
            }
        }
    }

    private static String makeFieldName(Schema record, Field field) {
        return field.name() + " in record " + record.getFullName();
    }

    private static void checkTypeEvolution(Schema s1,
                                           Schema s2,
                                           String parentPrefixMsg,
                                           List<String> errors,
                                           List<String> warnings,
                                           Set<SchemaPair> visitedSet) {

        /*
         * If Types are unequal, we normally just add an error.  However,
         * certain cases are handled specially: type promotions, and the two
         * cases where one of the two types is a union.
         */
        final Type type = s1.getType();
        final Type type2 = s2.getType();

        if (type != type2) {

            if (type == Type.UNION) {

                /*
                 * Changing a union to a non-union is an error except in the
                 * minor case where the new type is the sole member of the old
                 * union.
                 */
                final String s2Name = s2.getFullName();
                final Map<String, Schema> s1Types = getUnionTypes(s1);
                if (s1Types.size() == 1 && s1Types.containsKey(s2Name)) {
                    /* When types match, check for additional problems. */
                    checkTypeEvolution(s1Types.get(s2Name), s2,
                                       parentPrefixMsg, errors, warnings,
                                       visitedSet);
                } else {
                    errors.add(parentPrefixMsg + " Union with types " +
                               s1Types.keySet() +
                               " was changed to non-union type " + s2Name +
                               '.' + CANNOT_READ_WITH_NEW_SCHEMA);
                }

            } else if (type2 == Type.UNION) {

                /*
                 * Changing a non-union to a union is a warning when the new
                 * union includes the original type, and an error otherwise.
                 */
                final String s1Name = s1.getFullName();
                final Map<String, Schema> s2Types = getUnionTypes(s2);
                if (s2Types.size() == 1 && s2Types.containsKey(s1Name)) {
                    /* When types match, check for additional problems. */
                    checkTypeEvolution(s1, s2Types.get(s1Name),
                                       parentPrefixMsg, errors, warnings,
                                       visitedSet);
                } else if (s2Types.containsKey(s1Name)) {
                    warnings.add
                        (parentPrefixMsg + " Non-union type " + s1Name +
                         " was changed to a union with additional types " +
                         s2Types.keySet() + '.' + CANNOT_READ_WITH_OLD_SCHEMA);
                } else {
                    errors.add
                        (parentPrefixMsg + " Non-union type " + s1Name +
                         " was changed to a union that does not include the" +
                         " original type " + s2Types.keySet() + '.' +
                         CANNOT_READ_WITH_EITHER_SCHEMA);
                }

            } else if (isTypePromotion(type, type2)) {

                /* Type promotion is a warning rather than an error. */
                warnings.add
                    (parentPrefixMsg + " The type was promoted from " +
                     type + " to " + type2 + '.' +
                     CANNOT_READ_WITH_OLD_SCHEMA);
            } else {

                /* In all other cases, an error is added. */
                errors.add
                    (parentPrefixMsg +
                     " The type was changed incompatibly from " + type +
                     " to " + type2 + '.' +
                     (isTypePromotion(type2, type) ?
                      CANNOT_READ_WITH_NEW_SCHEMA :
                      CANNOT_READ_WITH_EITHER_SCHEMA));
            }

            /* We're done.  Checks below assume that Types are equal. */
            return;
        }

        /*
         * The named types (record, enum and fixed) are handled differently
         * than the others.  If the names of these types don't match, then we
         * add a field type error.  But additionally, further below in the
         * switch statement, we'll compare the two types and possibly add more
         * errors or warnings.  Prior to these latter checks, we return if
         * these types are alreadyVisited to prevent infinite recursion and
         * duplicate messages.
         */
        if (type == Type.RECORD || type == Type.ENUM || type == Type.FIXED) {
            final String n1 = s1.getFullName();
            final String n2 = s2.getFullName();
            if ((n1 == null) ? (n2 != null) : (!n1.equals(n2))) {
                errors.add
                    (parentPrefixMsg +
                     " The type name was changed incompatibly from " +
                     n1 + " to " + n2 + '.' + CANNOT_READ_WITH_EITHER_SCHEMA);
            }
            if (alreadyVisited(new SchemaPair(s1, s2), visitedSet)) {
                return;
            }
        }

        /*
         * The schema types and full names are the same, but other attributes
         * may be different.  Check for relevant attribute changes and
         * recursively check nested types.  Note that a recursive call to
         * checkTypeEvolution below will also call checkRecordEvolution if
         * applicable, and vice-versa.
         */
        switch (type) {
        case RECORD:
            /* Note that names were compared further above. */
            checkRecordEvolution(s1, s2, errors, warnings, visitedSet);
            break;
        case UNION:
            final Map<String, Schema> s1Types = getUnionTypes(s1);
            final Map<String, Schema> s2Types = getUnionTypes(s2);
            for (final Map.Entry<String, Schema> entry : s1Types.entrySet()) {
                final String typeName = entry.getKey();
                if (s2Types.get(typeName) == null) {
                    errors.add(parentPrefixMsg + " Union type " + typeName +
                               " was removed." + CANNOT_READ_WITH_NEW_SCHEMA);
                }
            }
            for (final Map.Entry<String, Schema> entry : s2Types.entrySet()) {
                final String typeName = entry.getKey();
                final Schema t2 = entry.getValue();
                final Schema t1 = s1Types.get(typeName);
                if (t1 == null) {
                    warnings.add(parentPrefixMsg + " Union type " + typeName +
                                 " was added." + CANNOT_READ_WITH_OLD_SCHEMA);
                } else {
                    /* Type names match, but there may be other problems. */
                    checkTypeEvolution(t1, t2, parentPrefixMsg, errors,
                                       warnings, visitedSet);
                }
            }
            break;
        case ARRAY:
            checkTypeEvolution
                (s1.getElementType(), s2.getElementType(),
                 parentPrefixMsg + " Array element type was changed.",
                 errors, warnings, visitedSet);
            break;
        case MAP:
            checkTypeEvolution
                (s1.getValueType(), s2.getValueType(),
                 parentPrefixMsg + " Map value type was changed.",
                 errors, warnings, visitedSet);
            break;
        case ENUM:
            /* Note that names were compared further above. */
            final Set<String> removedSymbols =
                new HashSet<String>(s1.getEnumSymbols());
            removedSymbols.removeAll(s2.getEnumSymbols());
            if (removedSymbols.size() > 0) {
                errors.add(parentPrefixMsg + " Enum symbols " +
                           removedSymbols.toString() + " were removed." +
                           CANNOT_READ_WITH_NEW_SCHEMA);
            }
            final Set<String> addedSymbols =
                new HashSet<String>(s2.getEnumSymbols());
            addedSymbols.removeAll(s1.getEnumSymbols());
            if (addedSymbols.size() > 0) {
                warnings.add(parentPrefixMsg + " Enum symbols " +
                             addedSymbols.toString() + " were added." +
                             CANNOT_READ_WITH_OLD_SCHEMA);
            }
            break;
        case FIXED:
            /* Note that names were compared further above. */
            if (s1.getFixedSize() != s2.getFixedSize()) {
                errors.add(parentPrefixMsg + " Fixed " + s1.getFullName() +
                           " size was changed from " + s1.getFixedSize() +
                           " to " + s2.getFixedSize() + '.' +
                           CANNOT_READ_WITH_EITHER_SCHEMA);
            }
            break;
        case STRING:
        case BYTES:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case NULL:
            break;
        default:
            /* Should never happen. */
            throw new RuntimeException("Unknown type: " + type);
        }
    }

    private static boolean isTypePromotion(Type t1, Type t2) {
        switch (t1) {
        case INT:
            return t2 == Type.LONG || t2 == Type.FLOAT || t2 == Type.DOUBLE;
        case LONG:
            return t2 == Type.FLOAT || t2 == Type.DOUBLE;
        case FLOAT:
            return t2 == Type.DOUBLE;
        default:
            return false;
        }
    }

    private static Map<String, Schema> getUnionTypes(Schema parent) {
        final Map<String, Schema> map = new HashMap<String, Schema>();
        for (final Schema s : parent.getTypes()) {
            map.put(s.getFullName(), s);
        }
        return map;
    }

    private static <T> boolean alreadyVisited(T visited,
                                              Set<T> visitedSet) {
        if (visitedSet.contains(visited)) {
            return true;
        }
        visitedSet.add(visited);
        return false;
    }

    /**
     * Contains two schemas that were compared earlier, so they can be placed
     * in the visited set.
     */
    private static class SchemaPair {

        private final Schema s1;
        private final Schema s2;

        SchemaPair(Schema s1, Schema s2) {
            this.s1 = s1;
            this.s2 = s2;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof SchemaPair)) {
                return false;
            }
            final SchemaPair o = (SchemaPair) other;
            return (s1 == o.s1 && s2 == o.s2) ||
                   (s1 == o.s2 && s2 == o.s1);
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(s1) + System.identityHashCode(s2);
        }
    }
}
