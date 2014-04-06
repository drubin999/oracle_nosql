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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Manage and bundle up a set of ParameterMaps. Used to transport multiple
 * ParameterMaps across the network, and to load parameters from a
 * configuration file. Note that each map has a type associated with it.
 *
 * The config.xml represents a simple"schema""
 * <config version="1">
 *   <component name="blah" type="paramtype">
 *     <property name="n" value="v" type="t"/>
 *     <property name="n" value="v" type="t"/>
 *     ...
 *   </component>
 *   <component name="foo">
 *     ...
 *   </component>
 *   ...
 * </config>
 */
public class LoadParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<ParameterMap> maps;
    private int version;

    public LoadParameters() {
        maps = new ArrayList<ParameterMap>();
        version = ParameterState.PARAMETER_VERSION;
    }

    public static LoadParameters getParameters(File file, Logger logger) {
        LoadParameters lp = new LoadParameters();
        lp.load(file, false, true, logger);
        return lp;
    }

    public static LoadParameters getParametersByType(File file) {
        return getParametersByType(file, null);
    }

    public static LoadParameters getParametersByType(File file, Logger logger) {
        LoadParameters lp = new LoadParameters();
        lp.load(file, true, true, logger);
        return lp;
    }

    /**
     * In order to make this modification more atomic, write first to a
     * temporary file and rename.  Because of file system semantics on
     * some platforms the rename can fail.  Handle that with a retry.
     * This will only fail if there is a reader.  Writers must be
     * synchronized by the callers (e.g. the SNA).
     */
    public void saveParameters(File file) {
        PrintWriter writer = null;
        File temp = null;
        try {
            temp = File.createTempFile(file.getName(), null,
                                       file.getParentFile());
            FileOutputStream fos = new FileOutputStream(temp);
            writer = new PrintWriter(fos);
            writer.printf("<config version=\"%d\">\n", version);
            for (ParameterMap map : maps) {
                map.write(writer);
            }
            writer.printf("</config>\n");
        } catch (Exception e) {
            throw new IllegalStateException("Problem creating config file: " +
                                            temp + ": " + e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        if (temp != null) {
            int nretries = 5;
            int sleepMs = 200;
            for (int i = 0; i < nretries; i++) {
                if (temp.renameTo(file)) {
                    return;
                }

                /**
                 * It seems that on Windows renameTo() often fails without a
                 * delete.  This shows up in tests, at least.  This is not a
                 * high performance path, so if it fails once, do the explicit
                 * delete on Windows.  This should not be unconditional because
                 * it makes the operation less atomic, leaving a small window
                 * available for a read failure.
                 */
                String os = System.getProperty("os.name");
                if (os.indexOf("Windows") != -1) {
                    file.delete();
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (Exception ignored) {
                }
            }
        }
        throw new IllegalStateException("Unable to save config file: " + file);
    }

    public List<ParameterMap> getMaps() {
        return maps;
    }

    public void addMap(ParameterMap map) {
        if (map != null) {
            maps.add(map);
        }
    }

    public ParameterMap removeMap(String name) {
        ParameterMap map = getMap(name);
        if (map != null) {
            maps.remove(map);
        }
        return map;
    }

    public ParameterMap removeMapByType(String type) {
        ParameterMap map = getMapByType(type);
        if (map != null) {
            maps.remove(map);
        }
        return map;
    }

    public ParameterMap getMap(String name, String type) {
        for (ParameterMap map : maps) {
            if (name.equals(map.getName()) && type.equals(map.getType())) {
                return map;
            }
        }
        return null;
    }

    public ParameterMap getMapByType(String type) {
        for (ParameterMap map : maps) {
            if (type.equals(map.getType())) {
                return map;
            }
        }
        return null;
    }

    public List<ParameterMap> getAllMaps(String type) {
        ArrayList<ParameterMap> list = new ArrayList<ParameterMap>();
        for (ParameterMap map : maps) {
            if (type.equals(map.getType())) {
                list.add(map);
            }
        }
        return list;
    }

    /**
     * Type is ignored for this variant.
     */
    public ParameterMap getMap(String name) {
        for (ParameterMap map : maps) {
            if (name.equals(map.getName())) {
                return map;
            }
        }
        return null;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    private void load(File file, boolean useTypes,
                      boolean ignoreUnknown, Logger logger) {
        InputStream is = null;
        try {
            URL url = file.toURI().toURL();
            SAXParserFactory factory = SAXParserFactory.newInstance();
            XMLReader xr = factory.newSAXParser().getXMLReader();
            ConfigHandler handler =
                new ConfigHandler(useTypes, ignoreUnknown, this, logger);
            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);
            is = url.openStream();
            xr.parse(new InputSource(is));
        } catch (SAXParseException e) {
            String msg = "Error while parsing line " + e.getLineNumber() +
                    " of " + file + ": " + e.getMessage();
            throw new IllegalStateException(msg);
        } catch (SAXException e) {
            throw new IllegalStateException("Problem with XML: " + e);
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e.getMessage());
        } catch (MalformedURLException me) {
            throw new IllegalStateException
                ("Could not translate file to URL: " + file);
        } catch (IOException io) {
            throw new IllegalStateException
                ("IOException parsing file: " + file + ": " + io);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    static class ConfigHandler extends DefaultHandler {
        Locator locator;
        StringBuilder curItem;
        ParameterMap curMap;
        boolean useTypes;
        boolean ignoreUnknown;
        Logger logger;
        LoadParameters lp;

        public ConfigHandler(boolean useTypes,
                             boolean ignoreUnknown,
                             LoadParameters lp,
                             Logger logger) {
            this.logger = logger;
            this.useTypes = useTypes;
            this.ignoreUnknown = ignoreUnknown;
            this.lp = lp;
        }

        @Override
        public void startElement(String uri,
                                 String localName,
                                 String qName,
                                 Attributes attributes)
            throws SAXException {

            if (qName.equals("config")) {
                /* get version */
                String stringVersion = attributes.getValue("version");
                if (stringVersion == null) {
                    throw new SAXParseException
                        ("config element must specify version",
                         locator);
                }
                lp.setVersion(Integer.parseInt(stringVersion));
            } else if (qName.equals("component")) {
                String curComponent = attributes.getValue("name");
                String curType = attributes.getValue("type");
                String validateString = attributes.getValue("validate");
                boolean validate = true;
                if (validateString != null) {
                    validate = Boolean.parseBoolean(validateString);
                }
                curMap = new ParameterMap(curComponent, curType,
                                          validate, lp.getVersion());
                /* Check for a badly formed component tag. */
                if (curComponent == null || curType == null) {
                    throw new SAXParseException
                        ("component element must specify name and type",
                         locator);
                }
            } else if (qName.equals("property")) {
                String name = attributes.getValue("name");
                String value = attributes.getValue("value");
                if (attributes.getLength() < 2 ||
                    attributes.getLength() > 3 ||
                    name == null ||
                    value == null) {
                    throw new SAXParseException
                        ("property element must only have 'name', 'value'" +
                         " and (optional) 'type' attributes", locator);
                }

                if (curMap == null) {
                    throw new SAXParseException
                        ("property elements are not allowed at global scope",
                         locator);
                }

                if (curMap.exists(name)) {
                    throw new SAXParseException
                        ("Duplicate property: " + name, locator);
                }

                Parameter param = null;
                if (useTypes) {
                    String type = attributes.getValue("type");
                    if (type == null) {
                        throw new SAXParseException
                            ("type attribute required on property element in" +
                             " this path", locator);
                    }
                    param = Parameter.createKnownType(name, value, type);
                    if (param != null) {
                        curMap.put(param);
                    } else if (logger != null) {
                        logger.warning("Could not create parameter: " +
                                       name);
                    }
                } else {
                    if (!curMap.setParameter(name, value, ignoreUnknown)) {
                        if (logger != null) {
                            logger.warning("Ignoring unknown parameter: " +
                                           name);
                        }
                    }
                }
            } else {
                throw new SAXParseException
                    ("Unknown element '" + qName + "'", locator);
            }
        }

        @Override
        public void characters(char ch[], int start, int length)
            throws SAXParseException {
            /* ignore content and whitespace */
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXParseException {

            if (qName.equals("component")) {
                lp.addMap(curMap);
                curMap = null;
            } else if (qName.equals("property")) {
                /* nothing to do */
            }
        }

        @Override
        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }
    }
}
