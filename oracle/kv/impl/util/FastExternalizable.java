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

package oracle.kv.impl.util;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Implemented by classes that are serialized for transfer over RMI using an
 * optimized technique that avoids the performance pitfalls of the standard
 * Serializable and Externalizable approaches.  It is mainly intended for API
 * operations, but may also be useful for high frequency operations outside of
 * the API.
 *
 * === Standard 'Slow' Serialization ===
 *
 * The simplest way to serialize objects passed and returned over RMI is to
 * make them Serializable.  The class implements the Serializable tag interface
 * and a serialVersionUID field must be declared as follows:
 *
 *  private static final long serialVersionUID = 1;
 *
 * With standard Serialization, for every object serialized in an RMI request
 * or response, metadata with a complete description of its fields is included
 * in the request/response.  The metadata is large and redundant among
 * requests, and it takes time to serialize and deserialize.  The
 * deserialization process is particularly lengthy because classes must be
 * looked up and the class metadata compared to the metadata passed in the
 * request/response.
 *
 * Because of its overhead, Serializable is not appropriate for high frequency
 * operations.  However, it is fine and very practical for low frequency
 * operations.
 *
 * The Externalizable interface is an alternative to Serializable.  Like
 * Serializable, the class implements the Externalizable tag interface and a
 * serialVersionUID field must be declared.  In addition, a public no-args
 * constructor must be declared.  With Externalizable, the class must implement
 * the writeExternal and readExternal methods to explicitly serialize and
 * deserialize each field.  The use of Externalizable is therefore much more
 * complex than with Serializable and requires maintenance.
 *
 * Unlike Serializable, the metadata for an Externalizable object does not
 * include a description of its fields.  Instead, only the class name and some
 * per-class metadata is included.  It is consequently much more efficient than
 * Serializable.  However, like with Serializable, a class lookup is performed
 * during deserialization.  In performance tests, the class lookup was found to
 * be have significant overhead.
 *
 * When using Externalizable, the class description and lookup overhead is
 * multiplied by the number of Externalizable classes for instances embedded in
 * the RMI request/response.  This overhead motivated the implementation of a
 * faster method of serialization.
 *
 * === Optimized or 'Fast' Serialization ===
 *
 * To implement fully optimized serialization for an RMI interface, the top
 * level objects (parameters and return value) of the remote methods should be
 * Externalizable.  RMI calls ObjectOutput.writeObject and
 * ObjectInput.readObject on every non-primitive parameter and return value, so
 * the classes must be Serializable or Externalizable, and Externalizable is
 * much preferred.
 *
 * The classes of objects contained by (embedded in) the top level parameter
 * and return value objects may be FastExternalizable rather than
 * Externalizable.  This avoids writing the class name of each object, and more
 * importantly the class lookup during deserialization.
 *
 * Like Externalizable, a FastExternalizable class reads and writes its
 * representation directly, a field at a time.  Unlike Serializable, a complete
 * description of the class is not included in the serialized form.
 *
 * Unlike Externalizable, a FastExternalizable class does not write a
 * full class name, and does not do a class lookup or use reflection to create
 * the instance during deserialization.  Instead, a simple integer constant is
 * typically used to identify the class, or in some cases the class is implied
 * from the context, i.e., the field type.  With FastExternalizable, an
 * instance of the class is created directly (with 'new') rather than using
 * reflection, or a constant instance may be used.
 *
 * Unlike Serializable and Externalizable, a FastExternalizable instance cannot
 * be written using ObjectOutput.writeObject or read using
 * ObjectInput.readObject, since these methods are reserved for standard
 * serialization.
 *
 * === The FastExternalizable serialVersion parameter ===
 *
 * Remote method input parameters must include a version identifying their
 * serialized format as well as the required serialized format of the return
 * value.  This allows an older client to interact with a newer service.  When
 * the serialization format changes, the service is responsible for supporting
 * older formats and serializing the return value in the format specified by
 * the client's input parameters.  The client is responsible for using the
 * minimum of the version it supports and the version supported by the service,
 * which allows a newer client to interact with an older service.
 *
 * TODO: More details to follow, as the negotiation of the version to use is not
 * fully implemented yet.
 *
 * === The FastExternalizable.writeFastExternal method ===
 *
 * With FastExternalizable, a member method with the same signature (other than
 * method name) as Externalizable.writeExternal is used to write the object.
 * The enclosing class (whether Externalizable or FastExternalizable itself)
 * should call writeFastExternal directly.
 *
 * If a class identifier field is stored for use by a factory method (more on
 * this below) it is written by the writeFastExternal method.
 *
 * === The FastExternalizable constructor ===
 *
 * Unlike Externalizable, a constructor rather than a method is used to read
 * the object from a ObjectInput.  The constructor signature has the same
 * signature (although we're comparing a method and a constructor) as
 * Externalizable.readExternal.
 *
 * A constructor is used rather than a method so that the fields it initializes
 * can be declared 'final' and a no-args constructor is not needed.
 *
 * A constructor signature similar to the following is required in all
 * FastExternalizable classes, by convention.  When a factory method is used,
 * an additional parameter may be required to initialize a class identifier
 * field (more on this below).
 *
 *   ExampleClass(ObjectInput in, int serialVersion) throws IOException {
 *       ...
 *   }
 *
 * If a field may contain instances of only a single class, not its subclasses,
 * and this won't be changing in the future, the constructor can be used
 * directly by the enclosing class to create the instance (with 'new') and read
 * it from the ObjectInput.  Otherwise, a factory method is used as described
 * next.
 *
 * === The FastExternalizable factory method ===
 *
 * If the class is variable (for example, subtypes of the field type may be
 * used) then a static factory method should be defined to create the instance
 * of the correct class and invoke its constructor, following the constructor
 * convention above.
 *
 * A factory method should also be used when the field may contain constant
 * values.  This allows the factory method to return predefined constants
 * rather than creating a new instance.
 *
 * By convention the signature of the factory method is:
 *
 *   static ExampleClass readFastExternal(ObjectInput in, int serialVersion) throws IOException {
 *       ...
 *   }
 *
 * A factory method must first read a class or constant identifier field of
 * some kind.  After reading the identifier field, it can create a new instance
 * of the appropriate class, or perhaps return a constant instance.
 *
 * A common technique for storing a class or instance identifier is to define
 * an Enum class with one value for each class or constant instance, and store
 * the Enum.ordinal() as the identifier.  When reading the identifier the
 * matching Enum is found, and an Enum method may be used to create an instance
 * of the appropriate class or return a constant instance.  Note that Enum
 * values used in this way may not be reordered, or the change in ordinals will
 * break serialization compatibility.
 *
 * When the factory constructs a new instance, it may pass the class identifier
 * (in addition to the ObjectInput parameter) to the constructor, if the class
 * identifier will be stored in an instance field.  The constructor cannot read
 * the class identifier from the ObjectInput, since it has already been read by
 * the factory method.
 *
 * === Null values ===
 *
 * When an object may be null, by convention we use one byte to indicate this,
 * where 1 means non-null, and 0 means null.  This logic is implemented in the
 * enclosing class for the object that may be null.
 *
 * === Combining FastExternalizable with other serialization approaches ===
 *
 * FastExternalizable may be combined with DPL-persistence, Serializable, or
 * Externalizable in a single class.  So far this has only been done for
 * DPL-persistence and Serializable, and this is straightforward.  It is
 * possible to combine FastExternalizable with Externalizable, but the need for
 * this has not yet arisen.
 *
 * For whatever combination is used, the most restrictive rule must be applied:
 *
 *  + A serialVersionUID must be included for Serializable and Externalizable,
 *    but not FastExternalizable or DPL-persistence.
 *
 *  + A no-args constructor must be included for DPL-persistence and
 *    Externalizable, but not Serializable or FastExternalizable.  The no-args
 *    constructor must be public for Externalizable, but may be private for
 *    DPL-persistence.
 *
 *  + Serialized fields must not be 'final' for DPL-persistence and
 *    Externalizable, but may be 'final' for Serializable and
 *    FastExternalizable.
 *
 * If ObjectInput.readObject is called downstream from a FastExternalizable
 * constructor or factory method, then ClassNotFoundException must be added to
 * the throws clause (in addition to IOException) on the constructor or factory
 * method, to propagate the exception upward.  This is necessary when combining
 * FastExternalizable with Serializable or Externalizable in certain cases, not
 * in the same class but in the same object hierarchy.  This situation has not
 * yet arisen, but could be handled as described.
 *
 * === Examples ===
 *
 * c.s.kv.impl.request.Request
 *
 *   Externalizable class that contains FastExternalizable instances.  Is a
 *   top-level parameter of the RMI remote method
 *   c.s.kv.impl.request.RequestHandler.execute.
 *
 * c.s.kv.Value
 *
 *   FastExternalizable class that includes a constructor but not a factory
 *   method.  Simplest case.
 *
 * c.s.kv.Consistency
 *
 *   FastExternalizable class that includes a factory method for the purpose
 *   of returning a constant instance in some cases.
 *
 * c.s.kv.impl.request.RequestResult.ValueVersionResult
 *
 *   Example of how null embedded objects are handled.
 *
 * c.s.kv.impl.Operation
 *
 *   FastExternalizable class that includes a constructor and factory method
 *   for creating Operation subclasses.  Uses an enum as the class identifier.
 *   c.s.kv.impl.Get is an example of a Operation subclass.
 *
 * c.s.kv.impl.admin.ResourceId
 *
 *   Similar to Operation but is additionally Serializable.  Several subclasses
 *   are also DPL-persistent.
 */
public interface FastExternalizable {

    /**
     * Writes the FastExternalizable object to the ObjectOutput.  To read the
     * object, use a constructor or factory method as described in the class
     * comments.
     */
    public void writeFastExternal(ObjectOutput out, short serialVersion)
        throws IOException;
}
