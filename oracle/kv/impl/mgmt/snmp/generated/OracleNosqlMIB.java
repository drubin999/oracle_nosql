package oracle.kv.impl.mgmt.snmp.generated;

//
// Generated by mibgen version 5.1 (03/08/07) when compiling OracleNosqlMIB in standard metadata mode.
//

// java imports
//
import java.io.Serializable;
import java.util.Hashtable;

// jmx imports
//
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.InstanceAlreadyExistsException;

// jdmk imports
//
import com.sun.management.snmp.agent.*;

/**
 * The class is used for representing "OracleNosqlMIB".
 * You can edit the file if you want to modify the behavior of the MIB.
 */
@SuppressWarnings({"unchecked","hiding","rawtypes","serial","unused"})
public class OracleNosqlMIB extends SnmpMib implements Serializable {

    /**
     * Default constructor. Initialize the Mib tree.
     */
    public OracleNosqlMIB() {
        mibName = "OracleNosqlMIB";
    }

    /**
     * Initialization of the MIB with no registration in Java DMK.
     */
    @Override
    public void init() throws IllegalAccessException {
        // Allow only one initialization of the MIB.
        //
        if (isInitialized == true) {
            return ;
        }

        try  {
            populate(null, null);
        } catch(IllegalAccessException x)  {
            throw x;
        } catch(RuntimeException x)  {
            throw x;
        } catch(Exception x)  {
            throw new Error(x.getMessage());
        }

        isInitialized = true;
    }

    /**
     * Initialization of the MIB with AUTOMATIC REGISTRATION in Java DMK.
     */
    @Override
    public ObjectName preRegister(MBeanServer server, ObjectName name)
            throws Exception {
        // Allow only one initialization of the MIB.
        //
        if (isInitialized == true) {
            throw new InstanceAlreadyExistsException();
        }

        // Initialize MBeanServer information.
        //
        this.server = server;

        populate(server, name);

        isInitialized = true;
        return name;
    }

    /**
     * Initialization of the MIB with no registration in Java DMK.
     */
    public void populate(MBeanServer server, ObjectName name) 
        throws Exception {
        // Allow only one initialization of the MIB.
        //
        if (isInitialized == true) {
            return ;
        }

        if (objectserver == null) 
            objectserver = new SnmpStandardObjectServer();

        // Initialization of the "Admin" group.
        // To disable support of this group, redefine the 
        // "createAdminMetaNode()" factory method, and make it return "null"
        //
        initAdmin(server);

        // Initialization of the "RepNodes" group.
        // To disable support of this group, redefine the 
        // "createRepNodesMetaNode()" factory method, and make it return "null"
        //
        initRepNodes(server);

        // Initialization of the "StorageNode" group.
        // To disable support of this group, redefine the 
        // "createStorageNodeMetaNode()" factory method, and make it return "null"
        //
        initStorageNode(server);

        isInitialized = true;
    }


    // ------------------------------------------------------------
    // 
    // Initialization of the "Admin" group.
    // 
    // ------------------------------------------------------------


    /**
     * Initialization of the "Admin" group.
     * 
     * To disable support of this group, redefine the 
     * "createAdminMetaNode()" factory method, and make it return "null"
     * 
     * @param server    MBeanServer for this group (may be null)
     * 
     **/
    protected void initAdmin(MBeanServer server) 
        throws Exception {
        final String oid = getGroupOid("Admin", "1.3.6.1.4.1.111.42.5");
        ObjectName objname = null;
        if (server != null) {
            objname = getGroupObjectName("Admin", oid, mibName + ":name=oracle.kv.impl.mgmt.snmp.generated.Admin");
        }
        final AdminMeta meta = createAdminMetaNode("Admin", oid, objname, server);
        if (meta != null) {
            meta.registerTableNodes( this, server );

            // Note that when using standard metadata,
            // the returned object must implement the "AdminMBean"
            // interface.
            //
            final AdminMBean group = (AdminMBean) createAdminMBean("Admin", oid, objname, server);
            meta.setInstance( group );
            registerGroupNode("Admin", oid, objname, meta, group, server);
        }
    }


    /**
     * Factory method for "Admin" group metadata class.
     * 
     * You can redefine this method if you need to replace the default
     * generated metadata class with your own customized class.
     * 
     * @param groupName Name of the group ("Admin")
     * @param groupOid  OID of this group
     * @param groupObjname ObjectName for this group (may be null)
     * @param server    MBeanServer for this group (may be null)
     * 
     * @return An instance of the metadata class generated for the
     *         "Admin" group (AdminMeta)
     * 
     **/
    protected AdminMeta createAdminMetaNode(String groupName,
                String groupOid, ObjectName groupObjname, MBeanServer server)  {
        return new AdminMeta(this, objectserver);
    }


    /**
     * Factory method for "Admin" group MBean.
     * 
     * You can redefine this method if you need to replace the default
     * generated MBean class with your own customized class.
     * 
     * @param groupName Name of the group ("Admin")
     * @param groupOid  OID of this group
     * @param groupObjname ObjectName for this group (may be null)
     * @param server    MBeanServer for this group (may be null)
     * 
     * @return An instance of the MBean class generated for the
     *         "Admin" group (Admin)
     * 
     * Note that when using standard metadata,
     * the returned object must implement the "AdminMBean"
     * interface.
     **/
    protected Object createAdminMBean(String groupName,
                String groupOid,  ObjectName groupObjname, MBeanServer server)  {

        // Note that when using standard metadata,
        // the returned object must implement the "AdminMBean"
        // interface.
        //
        if (server != null) 
            return new Admin(this,server);
        return new Admin(this);
    }


    // ------------------------------------------------------------
    // 
    // Initialization of the "RepNodes" group.
    // 
    // ------------------------------------------------------------


    /**
     * Initialization of the "RepNodes" group.
     * 
     * To disable support of this group, redefine the 
     * "createRepNodesMetaNode()" factory method, and make it return "null"
     * 
     * @param server    MBeanServer for this group (may be null)
     * 
     **/
    protected void initRepNodes(MBeanServer server) 
        throws Exception {
        final String oid = getGroupOid("RepNodes", "1.3.6.1.4.1.111.42.3");
        ObjectName objname = null;
        if (server != null) {
            objname = getGroupObjectName("RepNodes", oid, mibName + ":name=oracle.kv.impl.mgmt.snmp.generated.RepNodes");
        }
        final RepNodesMeta meta = createRepNodesMetaNode("RepNodes", oid, objname, server);
        if (meta != null) {
            meta.registerTableNodes( this, server );

            // Note that when using standard metadata,
            // the returned object must implement the "RepNodesMBean"
            // interface.
            //
            final RepNodesMBean group = (RepNodesMBean) createRepNodesMBean("RepNodes", oid, objname, server);
            meta.setInstance( group );
            registerGroupNode("RepNodes", oid, objname, meta, group, server);
        }
    }


    /**
     * Factory method for "RepNodes" group metadata class.
     * 
     * You can redefine this method if you need to replace the default
     * generated metadata class with your own customized class.
     * 
     * @param groupName Name of the group ("RepNodes")
     * @param groupOid  OID of this group
     * @param groupObjname ObjectName for this group (may be null)
     * @param server    MBeanServer for this group (may be null)
     * 
     * @return An instance of the metadata class generated for the
     *         "RepNodes" group (RepNodesMeta)
     * 
     **/
    protected RepNodesMeta createRepNodesMetaNode(String groupName,
                String groupOid, ObjectName groupObjname, MBeanServer server)  {
        return new RepNodesMeta(this, objectserver);
    }


    /**
     * Factory method for "RepNodes" group MBean.
     * 
     * You can redefine this method if you need to replace the default
     * generated MBean class with your own customized class.
     * 
     * @param groupName Name of the group ("RepNodes")
     * @param groupOid  OID of this group
     * @param groupObjname ObjectName for this group (may be null)
     * @param server    MBeanServer for this group (may be null)
     * 
     * @return An instance of the MBean class generated for the
     *         "RepNodes" group (RepNodes)
     * 
     * Note that when using standard metadata,
     * the returned object must implement the "RepNodesMBean"
     * interface.
     **/
    protected Object createRepNodesMBean(String groupName,
                String groupOid,  ObjectName groupObjname, MBeanServer server)  {

        // Note that when using standard metadata,
        // the returned object must implement the "RepNodesMBean"
        // interface.
        //
        if (server != null) 
            return new RepNodes(this,server);
        return new RepNodes(this);
    }


    // ------------------------------------------------------------
    // 
    // Initialization of the "StorageNode" group.
    // 
    // ------------------------------------------------------------


    /**
     * Initialization of the "StorageNode" group.
     * 
     * To disable support of this group, redefine the 
     * "createStorageNodeMetaNode()" factory method, and make it return "null"
     * 
     * @param server    MBeanServer for this group (may be null)
     * 
     **/
    protected void initStorageNode(MBeanServer server) 
        throws Exception {
        final String oid = getGroupOid("StorageNode", "1.3.6.1.4.1.111.42.1");
        ObjectName objname = null;
        if (server != null) {
            objname = getGroupObjectName("StorageNode", oid, mibName + ":name=oracle.kv.impl.mgmt.snmp.generated.StorageNode");
        }
        final StorageNodeMeta meta = createStorageNodeMetaNode("StorageNode", oid, objname, server);
        if (meta != null) {
            meta.registerTableNodes( this, server );

            // Note that when using standard metadata,
            // the returned object must implement the "StorageNodeMBean"
            // interface.
            //
            final StorageNodeMBean group = (StorageNodeMBean) createStorageNodeMBean("StorageNode", oid, objname, server);
            meta.setInstance( group );
            registerGroupNode("StorageNode", oid, objname, meta, group, server);
        }
    }


    /**
     * Factory method for "StorageNode" group metadata class.
     * 
     * You can redefine this method if you need to replace the default
     * generated metadata class with your own customized class.
     * 
     * @param groupName Name of the group ("StorageNode")
     * @param groupOid  OID of this group
     * @param groupObjname ObjectName for this group (may be null)
     * @param server    MBeanServer for this group (may be null)
     * 
     * @return An instance of the metadata class generated for the
     *         "StorageNode" group (StorageNodeMeta)
     * 
     **/
    protected StorageNodeMeta createStorageNodeMetaNode(String groupName,
                String groupOid, ObjectName groupObjname, MBeanServer server)  {
        return new StorageNodeMeta(this, objectserver);
    }


    /**
     * Factory method for "StorageNode" group MBean.
     * 
     * You can redefine this method if you need to replace the default
     * generated MBean class with your own customized class.
     * 
     * @param groupName Name of the group ("StorageNode")
     * @param groupOid  OID of this group
     * @param groupObjname ObjectName for this group (may be null)
     * @param server    MBeanServer for this group (may be null)
     * 
     * @return An instance of the MBean class generated for the
     *         "StorageNode" group (StorageNode)
     * 
     * Note that when using standard metadata,
     * the returned object must implement the "StorageNodeMBean"
     * interface.
     **/
    protected Object createStorageNodeMBean(String groupName,
                String groupOid,  ObjectName groupObjname, MBeanServer server)  {

        // Note that when using standard metadata,
        // the returned object must implement the "StorageNodeMBean"
        // interface.
        //
        if (server != null) 
            return new StorageNode(this,server);
        return new StorageNode(this);
    }


    // ------------------------------------------------------------
    // 
    // Implements the "registerTableMeta" method defined in "SnmpMib".
    // See the "SnmpMib" Javadoc API for more details.
    // 
    // ------------------------------------------------------------

    @Override
    public void registerTableMeta( String name, SnmpMibTable meta) {
        if (metadatas == null) return;
        if (name == null) return;
        metadatas.put(name,meta);
    }


    // ------------------------------------------------------------
    // 
    // Implements the "getRegisteredTableMeta" method defined in "SnmpMib".
    // See the "SnmpMib" Javadoc API for more details.
    // 
    // ------------------------------------------------------------

    @Override
    public SnmpMibTable getRegisteredTableMeta( String name ) {
        if (metadatas == null) return null;
        if (name == null) return null;
        return (SnmpMibTable) metadatas.get(name);
    }

    public SnmpStandardObjectServer getStandardObjectServer() {
        if (objectserver == null) 
            objectserver = new SnmpStandardObjectServer();
        return objectserver;
    }

    private boolean isInitialized = false;

    protected SnmpStandardObjectServer objectserver;

    protected final Hashtable metadatas = new Hashtable();
}
