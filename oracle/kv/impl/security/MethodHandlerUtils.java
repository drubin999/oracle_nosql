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
package oracle.kv.impl.security;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

/**
 * Common code for MethodHandler interaction
 */
final class MethodHandlerUtils {

    /* Not instantiable */
    private MethodHandlerUtils() {
    }

    /**
     * Call the method with the provided arguments.
     *
     * @param target the target object of an invocation
     * @param method a Method that should be called
     * @param args an argument list that should be passed to the method
     * @return an unspecified return type
     * @throws anything that the underlying method could produce, except that
     * anything that is not Error or Exception is wrapped in an
     * UndeclaredThrowableException.
     */
    static Object invokeMethod(Object target, Method method, Object[] args)
        throws Exception {
        try {
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException ite) {
                throw ite.getCause();
            }
        } catch (Exception e) {
            throw e;
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            throw new UndeclaredThrowableException(t);
        }
    }

    /**
     * An implementation of MethodHandler that provides basic method
     * invocation support.
     */
    static class DirectHandler implements MethodHandler {
        private final Object target;

        DirectHandler(Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(Method method, Object[] args)
            throws Exception {

            return invokeMethod(target, method, args);
        }
    }

    /**
     * An implementation of MethodHandler that provides basic method
     * invocation support by stripping the next-to-last argument from the
     * argument list.  This is used when calling an R2 implementation.
     */
    static class StripAuthCtxHandler implements MethodHandler {
        private final Object target;
        private final Method useMethod;

        /*
         * Constructor.
         *
         * @throw UnsupportedOperationException if the peer is running pre-R3
         * and a pre-R3 variant of the method being called cannot be found.
         */
        StripAuthCtxHandler(Object target, Method method) {
            this.target = target;

            final Class<?>[] newTypes =
                MethodHandlerUtils.stripAuthCtxArg(method.getParameterTypes());
            try {
                final Method newMethod =
                    target.getClass().getMethod(method.getName(), newTypes);

                this.useMethod = newMethod;
            } catch (NoSuchMethodException nsme) {
                throw new UnsupportedOperationException(
                    "Unable to call method " + method.getName() +
                    " on a pre-R3 implementation");
            }
        }

        @Override
        public Object invoke(Method method, Object[] args)
            throws Exception {

            return invokeMethod(target, useMethod, stripAuthCtxArg(args));
        }
    }

    /**
     * Create an array that contains all of the content of the input array
     * but with the next-to-last entry (expected to be AuthContext actual or
     * formal) removed.
     * The input array must contain at least 2 elements.
     */
    static <T> T[] stripAuthCtxArg(T[] args) {
        final T[] newArgs = Arrays.copyOfRange(args, 0, args.length - 1);
        newArgs[newArgs.length - 1] = args[args.length - 1];
        return newArgs;
    }
}
