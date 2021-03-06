package oracle.kv.impl.mgmt.snmp.generated;

//
// Generated by mibgen version 5.1 (03/08/07) when compiling OracleNosqlMIB in standard metadata mode.
//

// java imports
//
import java.io.Serializable;

// jmx imports
//
import javax.management.MBeanServer;
import com.sun.management.snmp.*;

// jdmk imports
//
import com.sun.management.snmp.agent.SnmpMibGroup;
import com.sun.management.snmp.agent.*;

/**
 * The class is used for representing SNMP metadata for the "RepNodes" group.
 * The group is defined with the following oid: 1.3.6.1.4.1.111.42.3.
 */
@SuppressWarnings({"serial","unused"})
public class RepNodesMeta extends SnmpMibGroup
     implements Serializable, SnmpStandardMetaServer {

    /**
     * Constructor for the metadata associated to "RepNodes".
     */
    public RepNodesMeta(SnmpMib myMib, SnmpStandardObjectServer objserv) {
        objectserver = objserv;
        try {
            registerObject(2);
            registerObject(1);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Get the value of a scalar variable
     */
    @Override
    public SnmpValue get(long var, Object data)
        throws SnmpStatusException {
        switch((int)var) {
            case 2: {
                throw new SnmpStatusException(SnmpStatusException.noSuchInstance);
                }

            case 1:
                return new SnmpInt(node.getNRepNodes());

            default:
                break;
        }
        throw new SnmpStatusException(SnmpStatusException.noSuchObject);
    }

    /**
     * Set the value of a scalar variable
     */
    @Override
    public SnmpValue set(SnmpValue x, long var, Object data)
        throws SnmpStatusException {
        switch((int)var) {
            case 2: {
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);
                }

            case 1:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            default:
                break;
        }
        throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);
    }

    /**
     * Check the value of a scalar variable
     */
    @Override
    public void check(SnmpValue x, long var, Object data)
        throws SnmpStatusException {
        switch((int) var) {
            case 2: {
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);
                }

            case 1:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            default:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);
        }
    }

    /**
     * Allow to bind the metadata description to a specific object.
     */
    protected void setInstance(RepNodesMBean var) {
        node = var;
    }


    // ------------------------------------------------------------
    // 
    // Implements the "get" method defined in "SnmpMibGroup".
    // See the "SnmpMibGroup" Javadoc API for more details.
    // 
    // ------------------------------------------------------------

    @Override
    public void get(SnmpMibSubRequest req, int depth)
        throws SnmpStatusException {
        objectserver.get(this,req,depth);
    }


    // ------------------------------------------------------------
    // 
    // Implements the "set" method defined in "SnmpMibGroup".
    // See the "SnmpMibGroup" Javadoc API for more details.
    // 
    // ------------------------------------------------------------

    @Override
    public void set(SnmpMibSubRequest req, int depth)
        throws SnmpStatusException {
        objectserver.set(this,req,depth);
    }


    // ------------------------------------------------------------
    // 
    // Implements the "check" method defined in "SnmpMibGroup".
    // See the "SnmpMibGroup" Javadoc API for more details.
    // 
    // ------------------------------------------------------------

    @Override
    public void check(SnmpMibSubRequest req, int depth)
        throws SnmpStatusException {
        objectserver.check(this,req,depth);
    }

    /**
     * Returns true if "arc" identifies a scalar object.
     */
    @Override
    public boolean isVariable(long arc) {

        switch((int)arc) {
            case 1:
                return true;
            default:
                break;
        }
        return false;
    }

    /**
     * Returns true if "arc" identifies a readable scalar object.
     */
@Override
    public boolean isReadable(long arc) {

        switch((int)arc) {
            case 1:
                return true;
            default:
                break;
        }
        return false;
    }


    // ------------------------------------------------------------
    // 
    // Implements the "skipVariable" method defined in "SnmpMibGroup".
    // See the "SnmpMibGroup" Javadoc API for more details.
    // 
    // ------------------------------------------------------------

    @Override
    public boolean  skipVariable(long var, Object data, int pduVersion) {
        return false;
    }

    /**
     * Return the name of the attribute corresponding to the SNMP variable identified by "id".
     */
    public String getAttributeName(long id)
        throws SnmpStatusException {
        switch((int)id) {
            case 2: {
                throw new SnmpStatusException(SnmpStatusException.noSuchInstance);
                }

            case 1:
                return "NRepNodes";

            default:
                break;
        }
        throw new SnmpStatusException(SnmpStatusException.noSuchObject);
    }

    /**
     * Returns true if "arc" identifies a table object.
     */
    @Override
    public boolean isTable(long arc) {

        switch((int)arc) {
            case 2:
                return true;
            default:
                break;
        }
        return false;
    }

    /**
     * Returns the table object identified by "arc".
     */
    @Override
    public SnmpMibTable getTable(long arc) {

        switch((int)arc) {
            case 2:
                return tableRepNodeTable;
        default:
            break;
        }
        return null;
    }

    /**
     * Register the group's SnmpMibTable objects with the meta-data.
     */
    public void registerTableNodes(SnmpMib mib, MBeanServer server) {
        tableRepNodeTable = createRepNodeTableMetaNode("RepNodeTable", "RepNodes", mib, server);
        if ( tableRepNodeTable != null)  {
            tableRepNodeTable.registerEntryNode(mib,server);
            mib.registerTableMeta("RepNodeTable", tableRepNodeTable);
        }

    }


    /**
     * Factory method for "RepNodeTable" table metadata class.
     * 
     * You can redefine this method if you need to replace the default
     * generated metadata class with your own customized class.
     * 
     * @param tableName Name of the table object ("RepNodeTable")
     * @param groupName Name of the group to which this table belong ("RepNodes")
     * @param mib The SnmpMib object in which this table is registered
     * @param server MBeanServer for this table entries (may be null)
     * 
     * @return An instance of the metadata class generated for the
     *         "RepNodeTable" table (RepNodeTableMeta)
     * 
     **/
    protected RepNodeTableMeta createRepNodeTableMetaNode(String tableName, String groupName, SnmpMib mib, MBeanServer server)  {
        return new RepNodeTableMeta(mib, objectserver);
    }

    protected RepNodesMBean node;
    protected SnmpStandardObjectServer objectserver = null;
    protected RepNodeTableMeta tableRepNodeTable = null;
}
