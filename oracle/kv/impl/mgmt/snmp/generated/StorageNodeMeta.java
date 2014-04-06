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
 * The class is used for representing SNMP metadata for the "StorageNode" group.
 * The group is defined with the following oid: 1.3.6.1.4.1.111.42.1.
 */
@SuppressWarnings({"serial","unused"})
public class StorageNodeMeta extends SnmpMibGroup
     implements Serializable, SnmpStandardMetaServer {

    /**
     * Constructor for the metadata associated to "StorageNode".
     */
    public StorageNodeMeta(SnmpMib myMib, SnmpStandardObjectServer objserv) {
        objectserver = objserv;
        try {
            registerObject(9);
            registerObject(8);
            registerObject(7);
            registerObject(6);
            registerObject(15);
            registerObject(5);
            registerObject(14);
            registerObject(13);
            registerObject(4);
            registerObject(3);
            registerObject(12);
            registerObject(2);
            registerObject(11);
            registerObject(10);
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
            case 9:
                return new SnmpInt(node.getSnAdminHttpPort());

            case 8:
                return new SnmpString(node.getSnRootDirPath());

            case 7:
                return new SnmpString(node.getSnStoreName());

            case 6:
                return new SnmpString(node.getSnHaPortRange());

            case 15:
                return new SnmpInt(node.getSnCPUs());

            case 5:
                return new SnmpString(node.getSnHAHostname());

            case 14:
                return new SnmpInt(node.getSnMemory());

            case 13:
                return new SnmpString(node.getSnMountPoints());

            case 4:
                return new SnmpInt(node.getSnRegistryPort());

            case 3:
                return new SnmpString(node.getSnHostname());

            case 12:
                return new SnmpInt(node.getSnCapacity());

            case 2:
                return new SnmpInt(node.getSnServiceStatus());

            case 11:
                return new SnmpInt(node.getSnLogFileLimit());

            case 10:
                return new SnmpInt(node.getSnLogFileCount());

            case 1:
                return new SnmpInt(node.getSnId());

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
            case 9:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 8:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 7:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 6:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 15:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 5:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 14:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 13:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 4:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 3:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 12:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 2:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 11:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 10:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

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
            case 9:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 8:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 7:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 6:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 15:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 5:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 14:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 13:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 4:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 3:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 12:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 2:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 11:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 10:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            case 1:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);

            default:
                throw new SnmpStatusException(SnmpStatusException.snmpRspNotWritable);
        }
    }

    /**
     * Allow to bind the metadata description to a specific object.
     */
    protected void setInstance(StorageNodeMBean var) {
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
            case 9:
            case 8:
            case 7:
            case 6:
            case 15:
            case 5:
            case 14:
            case 13:
            case 4:
            case 3:
            case 12:
            case 2:
            case 11:
            case 10:
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
            case 9:
            case 8:
            case 7:
            case 6:
            case 15:
            case 5:
            case 14:
            case 13:
            case 4:
            case 3:
            case 12:
            case 2:
            case 11:
            case 10:
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
            case 9:
                return "SnAdminHttpPort";

            case 8:
                return "SnRootDirPath";

            case 7:
                return "SnStoreName";

            case 6:
                return "SnHaPortRange";

            case 15:
                return "SnCPUs";

            case 5:
                return "SnHAHostname";

            case 14:
                return "SnMemory";

            case 13:
                return "SnMountPoints";

            case 4:
                return "SnRegistryPort";

            case 3:
                return "SnHostname";

            case 12:
                return "SnCapacity";

            case 2:
                return "SnServiceStatus";

            case 11:
                return "SnLogFileLimit";

            case 10:
                return "SnLogFileCount";

            case 1:
                return "SnId";

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
        return null;
    }

    /**
     * Register the group's SnmpMibTable objects with the meta-data.
     */
    public void registerTableNodes(SnmpMib mib, MBeanServer server) {
    }

    protected StorageNodeMBean node;
    protected SnmpStandardObjectServer objectserver = null;
}