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

import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.MessageFileProcessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Enumeration;
import java.util.ResourceBundle;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is responsible for loading the locale specific messages file so
 * that runtime messages can be constructed utilizing the appropriate locale.
 * This class only holds the stream open but does not cache any messages.
 */
public class NoSQLMessagesResourceBundle extends ResourceBundle {

    protected LineNumberReader messageFile = null;
    private Logger logger = null;
    private String resourceName = null;

    private NoSQLMessagesResourceBundle(LineNumberReader msgLnReader) {
	 this.messageFile = msgLnReader;
	 logger = LoggerUtils.getLogger(getClass(), getClass().getName());
    }

    public NoSQLMessagesResourceBundle() {

        logger = LoggerUtils.getLogger(getClass(), getClass().getName());

        /*
         * Call ResourceBundle.getBundle to find the appropriate locale based
         * messages file.  Once located, only maintain a line numbered reader
         * but do not instantiate a resource bundle.
         */
         NoSQLMessagesResourceBundle rb = (NoSQLMessagesResourceBundle)
	     ResourceBundle.getBundle
             (MessageFileProcessor.MESSAGES_FILE_BASE_NAME,
              new ResourceBundle.Control() {
                  @Override
                  public ResourceBundle newBundle(String baseName,
                                                  Locale locale,
                                                  String format,
                                                  ClassLoader loader,
                                                  boolean reload)
                      throws IllegalAccessException,
                             InstantiationException,
                             IOException {

                      final String bundleName = toBundleName(baseName, locale);
                      resourceName =
                          toResourceName(bundleName, MessageFileProcessor.
                                         MESSAGES_FILE_SUFFIX);

                      final InputStream stream =
                          loader.getResourceAsStream(resourceName);

                      if (stream != null) {
                          messageFile =
                              new LineNumberReader
                              (new BufferedReader
                               (new InputStreamReader(stream)));
                          return new NoSQLMessagesResourceBundle(messageFile);
                      }

                      throw new InstantiationException
                          ("Unable to locate " + baseName + " in classpath");
                  }
              });
	  logger = rb.logger;
	  messageFile = rb.messageFile;
    }

    /**
     * Returns all of the keys from the message file being wrapped by this
     * class. Not used, so not implemented.
     *
     * @return All keys found in the messages file
     */
    @Override
    public Enumeration<String> getKeys() {
        throw new RuntimeException
            (NoSQLMessagesResourceBundle.class.getName() +
             ".getKeys() called but not implemented!");
    }

    /**
     * Returns the message specified by key from this class's messages file.
     *
     * @param key The message key.
     *
     * @return The message associated with the key
     */
    @Override
    public Object handleGetObject(final String key)  {
        try {
            return MessageFileProcessor.getMessageForKey(key, messageFile);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                       "Unable to read message for key " + key +
                       " from message file " + resourceName +
                       " error: " + e.toString());
            return null;
        }
    }
}
