package oracle.kv.impl.mgmt.snmp.generated;

//
// Generated by mibgen version 5.1 (03/08/07) when compiling OracleNosqlMIB.
//

// java imports
//
import java.io.Serializable;

// jmx imports
//
import com.sun.management.snmp.SnmpStatusException;

// jdmk imports
//
import com.sun.management.snmp.agent.SnmpMib;

/**
 * The class is used for implementing the "RepNodeTableEntry" group.
 * The group is defined with the following oid: 1.3.6.1.4.1.111.42.3.2.1.
 */
@SuppressWarnings({"serial","unused"})
public class RepNodeTableEntry implements RepNodeTableEntryMBean, Serializable {

    /**
     * Variable for storing the value of "RepNodeLatencyCeiling".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.109".
     */
    protected Integer RepNodeLatencyCeiling = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMountPoint".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.108".
     */
    protected String RepNodeMountPoint = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeMultiIntervalPeriod".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.29".
     */
    protected Integer RepNodeMultiIntervalPeriod = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeHeapSize".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.107".
     */
    protected Integer RepNodeHeapSize = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeLoggingConfigProps".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.106".
     */
    protected String RepNodeLoggingConfigProps = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeMultiIntervalEnd".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.28".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeMultiIntervalEnd = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeMultiIntervalStart".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.27".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeMultiIntervalStart = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeJavaMiscParams".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.105".
     */
    protected String RepNodeJavaMiscParams = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeCumulativePct99".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.26".
     */
    protected Integer RepNodeCumulativePct99 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMaxTrackedLatency".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.104".
     */
    protected Integer RepNodeMaxTrackedLatency = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativePct95".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.25".
     */
    protected Integer RepNodeCumulativePct95 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeStatsInterval".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.103".
     */
    protected Integer RepNodeStatsInterval = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativeLatAvgFrac".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.24".
     */
    protected Integer RepNodeCumulativeLatAvgFrac = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCollectEnvStats".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.102".
     */
    protected EnumRepNodeCollectEnvStats RepNodeCollectEnvStats = new EnumRepNodeCollectEnvStats();

    /**
     * Variable for storing the value of "RepNodeCumulativeLatAvgInt".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.23".
     */
    protected Integer RepNodeCumulativeLatAvgInt = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeConfigProperties".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.101".
     */
    protected String RepNodeConfigProperties = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeCumulativeLatAvg".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.22".
     */
    protected String RepNodeCumulativeLatAvg = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeCacheSize".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.100".
     */
    protected Integer RepNodeCacheSize = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativeLatMax".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.21".
     */
    protected Integer RepNodeCumulativeLatMax = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativeLatMin".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.20".
     */
    protected Integer RepNodeCumulativeLatMin = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCommitLag".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.50".
     */
    protected Long RepNodeCommitLag = new Long(1);

    /**
     * Variable for storing the value of "RepNodeIntervalLatMin".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.9".
     */
    protected Integer RepNodeIntervalLatMin = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativeThroughput".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.19".
     */
    protected Long RepNodeCumulativeThroughput = new Long(1);

    /**
     * Variable for storing the value of "RepNodeIntervalThroughput".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.8".
     */
    protected Long RepNodeIntervalThroughput = new Long(1);

    /**
     * Variable for storing the value of "RepNodeIntervalTotalOps".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.7".
     */
    protected Integer RepNodeIntervalTotalOps = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativeTotalOps".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.18".
     */
    protected Integer RepNodeCumulativeTotalOps = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCumulativeEnd".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.17".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeCumulativeEnd = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeMultiCumulativePct99".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.49".
     */
    protected Integer RepNodeMultiCumulativePct99 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalPeriod".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.6".
     */
    protected Integer RepNodeIntervalPeriod = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiCumulativePct95".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.48".
     */
    protected Integer RepNodeMultiCumulativePct95 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalEnd".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.5".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeIntervalEnd = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeCumulativeStart".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.16".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeCumulativeStart = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeIntervalPct99".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.15".
     */
    protected Integer RepNodeIntervalPct99 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalStart".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.4".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeIntervalStart = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeLatAvgFrac".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.47".
     */
    protected Integer RepNodeMultiCumulativeLatAvgFrac = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalPct95".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.14".
     */
    protected Integer RepNodeIntervalPct95 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeServiceStatus".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.3".
     */
    protected EnumRepNodeServiceStatus RepNodeServiceStatus = new EnumRepNodeServiceStatus();

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeLatAvgInt".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.46".
     */
    protected Integer RepNodeMultiCumulativeLatAvgInt = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeNumber".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.2".
     */
    protected Integer RepNodeNumber = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalLatAvgFrac".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.13".
     */
    protected Integer RepNodeIntervalLatAvgFrac = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeLatAvg".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.45".
     */
    protected String RepNodeMultiCumulativeLatAvg = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeShardNumber".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.1".
     */
    protected Integer RepNodeShardNumber = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalLatAvgInt".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.12".
     */
    protected Integer RepNodeIntervalLatAvgInt = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeLatMax".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.44".
     */
    protected Integer RepNodeMultiCumulativeLatMax = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeIntervalLatAvg".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.11".
     */
    protected String RepNodeIntervalLatAvg = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeLatMin".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.43".
     */
    protected Integer RepNodeMultiCumulativeLatMin = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeThroughput".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.42".
     */
    protected Long RepNodeMultiCumulativeThroughput = new Long(1);

    /**
     * Variable for storing the value of "RepNodeIntervalLatMax".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.10".
     */
    protected Integer RepNodeIntervalLatMax = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeTotalOps".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.41".
     */
    protected Integer RepNodeMultiCumulativeTotalOps = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeEnd".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.40".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeMultiCumulativeEnd = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeStart".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.39".
     * In the SNMP MIB, this is defined as a fixed length string of size 8.
     */
    protected Byte[] RepNodeMultiCumulativeStart = { new Byte("74"), new Byte("68"), new Byte("77"), new Byte("75")};

    /**
     * Variable for storing the value of "RepNodeMultiIntervalPct99".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.38".
     */
    protected Integer RepNodeMultiIntervalPct99 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalPct95".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.37".
     */
    protected Integer RepNodeMultiIntervalPct95 = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalLatAvgFrac".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.36".
     */
    protected Integer RepNodeMultiIntervalLatAvgFrac = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalLatAvgInt".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.35".
     */
    protected Integer RepNodeMultiIntervalLatAvgInt = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeCommitLagThreshold".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.113".
     */
    protected Long RepNodeCommitLagThreshold = new Long(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalLatAvg".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.34".
     */
    protected String RepNodeMultiIntervalLatAvg = new String("JDMK 5.1");

    /**
     * Variable for storing the value of "RepNodeMultiCumulativeTotalRequests".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.112".
     */
    protected Integer RepNodeMultiCumulativeTotalRequests = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalLatMax".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.33".
     */
    protected Integer RepNodeMultiIntervalLatMax = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalTotalRequests".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.111".
     */
    protected Integer RepNodeMultiIntervalTotalRequests = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeThroughputFloor".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.110".
     */
    protected Integer RepNodeThroughputFloor = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalLatMin".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.32".
     */
    protected Integer RepNodeMultiIntervalLatMin = new Integer(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalThroughput".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.31".
     */
    protected Long RepNodeMultiIntervalThroughput = new Long(1);

    /**
     * Variable for storing the value of "RepNodeMultiIntervalTotalOps".
     * The variable is identified by: "1.3.6.1.4.1.111.42.3.2.1.30".
     */
    protected Integer RepNodeMultiIntervalTotalOps = new Integer(1);


    /**
     * Constructor for the "RepNodeTableEntry" group.
     */
    public RepNodeTableEntry(SnmpMib myMib) {
    }

    /**
     * Getter for the "RepNodeLatencyCeiling" variable.
     */
    @Override
    public Integer getRepNodeLatencyCeiling() throws SnmpStatusException {
        return RepNodeLatencyCeiling;
    }

    /**
     * Getter for the "RepNodeMountPoint" variable.
     */
    @Override
    public String getRepNodeMountPoint() throws SnmpStatusException {
        return RepNodeMountPoint;
    }

    /**
     * Getter for the "RepNodeMultiIntervalPeriod" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalPeriod() throws SnmpStatusException {
        return RepNodeMultiIntervalPeriod;
    }

    /**
     * Getter for the "RepNodeHeapSize" variable.
     */
    @Override
    public Integer getRepNodeHeapSize() throws SnmpStatusException {
        return RepNodeHeapSize;
    }

    /**
     * Getter for the "RepNodeLoggingConfigProps" variable.
     */
    @Override
    public String getRepNodeLoggingConfigProps() throws SnmpStatusException {
        return RepNodeLoggingConfigProps;
    }

    /**
     * Getter for the "RepNodeMultiIntervalEnd" variable.
     */
    @Override
    public Byte[] getRepNodeMultiIntervalEnd() throws SnmpStatusException {
        return RepNodeMultiIntervalEnd;
    }

    /**
     * Getter for the "RepNodeMultiIntervalStart" variable.
     */
    @Override
    public Byte[] getRepNodeMultiIntervalStart() throws SnmpStatusException {
        return RepNodeMultiIntervalStart;
    }

    /**
     * Getter for the "RepNodeJavaMiscParams" variable.
     */
    @Override
    public String getRepNodeJavaMiscParams() throws SnmpStatusException {
        return RepNodeJavaMiscParams;
    }

    /**
     * Getter for the "RepNodeCumulativePct99" variable.
     */
    @Override
    public Integer getRepNodeCumulativePct99() throws SnmpStatusException {
        return RepNodeCumulativePct99;
    }

    /**
     * Getter for the "RepNodeMaxTrackedLatency" variable.
     */
    @Override
    public Integer getRepNodeMaxTrackedLatency() throws SnmpStatusException {
        return RepNodeMaxTrackedLatency;
    }

    /**
     * Getter for the "RepNodeCumulativePct95" variable.
     */
    @Override
    public Integer getRepNodeCumulativePct95() throws SnmpStatusException {
        return RepNodeCumulativePct95;
    }

    /**
     * Getter for the "RepNodeStatsInterval" variable.
     */
    @Override
    public Integer getRepNodeStatsInterval() throws SnmpStatusException {
        return RepNodeStatsInterval;
    }

    /**
     * Getter for the "RepNodeCumulativeLatAvgFrac" variable.
     */
    @Override
    public Integer getRepNodeCumulativeLatAvgFrac() throws SnmpStatusException {
        return RepNodeCumulativeLatAvgFrac;
    }

    /**
     * Getter for the "RepNodeCollectEnvStats" variable.
     */
    @Override
    public EnumRepNodeCollectEnvStats getRepNodeCollectEnvStats() throws SnmpStatusException {
        return RepNodeCollectEnvStats;
    }

    /**
     * Getter for the "RepNodeCumulativeLatAvgInt" variable.
     */
    @Override
    public Integer getRepNodeCumulativeLatAvgInt() throws SnmpStatusException {
        return RepNodeCumulativeLatAvgInt;
    }

    /**
     * Getter for the "RepNodeConfigProperties" variable.
     */
    @Override
    public String getRepNodeConfigProperties() throws SnmpStatusException {
        return RepNodeConfigProperties;
    }

    /**
     * Getter for the "RepNodeCumulativeLatAvg" variable.
     */
    @Override
    public String getRepNodeCumulativeLatAvg() throws SnmpStatusException {
        return RepNodeCumulativeLatAvg;
    }

    /**
     * Getter for the "RepNodeCacheSize" variable.
     */
    @Override
    public Integer getRepNodeCacheSize() throws SnmpStatusException {
        return RepNodeCacheSize;
    }

    /**
     * Getter for the "RepNodeCumulativeLatMax" variable.
     */
    @Override
    public Integer getRepNodeCumulativeLatMax() throws SnmpStatusException {
        return RepNodeCumulativeLatMax;
    }

    /**
     * Getter for the "RepNodeCumulativeLatMin" variable.
     */
    @Override
    public Integer getRepNodeCumulativeLatMin() throws SnmpStatusException {
        return RepNodeCumulativeLatMin;
    }

    /**
     * Getter for the "RepNodeCommitLag" variable.
     */
    @Override
    public Long getRepNodeCommitLag() throws SnmpStatusException {
        return RepNodeCommitLag;
    }

    /**
     * Getter for the "RepNodeIntervalLatMin" variable.
     */
    @Override
    public Integer getRepNodeIntervalLatMin() throws SnmpStatusException {
        return RepNodeIntervalLatMin;
    }

    /**
     * Getter for the "RepNodeCumulativeThroughput" variable.
     */
    @Override
    public Long getRepNodeCumulativeThroughput() throws SnmpStatusException {
        return RepNodeCumulativeThroughput;
    }

    /**
     * Getter for the "RepNodeIntervalThroughput" variable.
     */
    @Override
    public Long getRepNodeIntervalThroughput() throws SnmpStatusException {
        return RepNodeIntervalThroughput;
    }

    /**
     * Getter for the "RepNodeIntervalTotalOps" variable.
     */
    @Override
    public Integer getRepNodeIntervalTotalOps() throws SnmpStatusException {
        return RepNodeIntervalTotalOps;
    }

    /**
     * Getter for the "RepNodeCumulativeTotalOps" variable.
     */
    @Override
    public Integer getRepNodeCumulativeTotalOps() throws SnmpStatusException {
        return RepNodeCumulativeTotalOps;
    }

    /**
     * Getter for the "RepNodeCumulativeEnd" variable.
     */
    @Override
    public Byte[] getRepNodeCumulativeEnd() throws SnmpStatusException {
        return RepNodeCumulativeEnd;
    }

    /**
     * Getter for the "RepNodeMultiCumulativePct99" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativePct99() throws SnmpStatusException {
        return RepNodeMultiCumulativePct99;
    }

    /**
     * Getter for the "RepNodeIntervalPeriod" variable.
     */
    @Override
    public Integer getRepNodeIntervalPeriod() throws SnmpStatusException {
        return RepNodeIntervalPeriod;
    }

    /**
     * Getter for the "RepNodeMultiCumulativePct95" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativePct95() throws SnmpStatusException {
        return RepNodeMultiCumulativePct95;
    }

    /**
     * Getter for the "RepNodeIntervalEnd" variable.
     */
    @Override
    public Byte[] getRepNodeIntervalEnd() throws SnmpStatusException {
        return RepNodeIntervalEnd;
    }

    /**
     * Getter for the "RepNodeCumulativeStart" variable.
     */
    @Override
    public Byte[] getRepNodeCumulativeStart() throws SnmpStatusException {
        return RepNodeCumulativeStart;
    }

    /**
     * Getter for the "RepNodeIntervalPct99" variable.
     */
    @Override
    public Integer getRepNodeIntervalPct99() throws SnmpStatusException {
        return RepNodeIntervalPct99;
    }

    /**
     * Getter for the "RepNodeIntervalStart" variable.
     */
    @Override
    public Byte[] getRepNodeIntervalStart() throws SnmpStatusException {
        return RepNodeIntervalStart;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeLatAvgFrac" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativeLatAvgFrac() throws SnmpStatusException {
        return RepNodeMultiCumulativeLatAvgFrac;
    }

    /**
     * Getter for the "RepNodeIntervalPct95" variable.
     */
    @Override
    public Integer getRepNodeIntervalPct95() throws SnmpStatusException {
        return RepNodeIntervalPct95;
    }

    /**
     * Getter for the "RepNodeServiceStatus" variable.
     */
    @Override
    public EnumRepNodeServiceStatus getRepNodeServiceStatus() throws SnmpStatusException {
        return RepNodeServiceStatus;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeLatAvgInt" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativeLatAvgInt() throws SnmpStatusException {
        return RepNodeMultiCumulativeLatAvgInt;
    }

    /**
     * Getter for the "RepNodeNumber" variable.
     */
    @Override
    public Integer getRepNodeNumber() throws SnmpStatusException {
        return RepNodeNumber;
    }

    /**
     * Getter for the "RepNodeIntervalLatAvgFrac" variable.
     */
    @Override
    public Integer getRepNodeIntervalLatAvgFrac() throws SnmpStatusException {
        return RepNodeIntervalLatAvgFrac;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeLatAvg" variable.
     */
    @Override
    public String getRepNodeMultiCumulativeLatAvg() throws SnmpStatusException {
        return RepNodeMultiCumulativeLatAvg;
    }

    /**
     * Getter for the "RepNodeShardNumber" variable.
     */
    @Override
    public Integer getRepNodeShardNumber() throws SnmpStatusException {
        return RepNodeShardNumber;
    }

    /**
     * Getter for the "RepNodeIntervalLatAvgInt" variable.
     */
    @Override
    public Integer getRepNodeIntervalLatAvgInt() throws SnmpStatusException {
        return RepNodeIntervalLatAvgInt;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeLatMax" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativeLatMax() throws SnmpStatusException {
        return RepNodeMultiCumulativeLatMax;
    }

    /**
     * Getter for the "RepNodeIntervalLatAvg" variable.
     */
    @Override
    public String getRepNodeIntervalLatAvg() throws SnmpStatusException {
        return RepNodeIntervalLatAvg;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeLatMin" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativeLatMin() throws SnmpStatusException {
        return RepNodeMultiCumulativeLatMin;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeThroughput" variable.
     */
    @Override
    public Long getRepNodeMultiCumulativeThroughput() throws SnmpStatusException {
        return RepNodeMultiCumulativeThroughput;
    }

    /**
     * Getter for the "RepNodeIntervalLatMax" variable.
     */
    @Override
    public Integer getRepNodeIntervalLatMax() throws SnmpStatusException {
        return RepNodeIntervalLatMax;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeTotalOps" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativeTotalOps() throws SnmpStatusException {
        return RepNodeMultiCumulativeTotalOps;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeEnd" variable.
     */
    @Override
    public Byte[] getRepNodeMultiCumulativeEnd() throws SnmpStatusException {
        return RepNodeMultiCumulativeEnd;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeStart" variable.
     */
    @Override
    public Byte[] getRepNodeMultiCumulativeStart() throws SnmpStatusException {
        return RepNodeMultiCumulativeStart;
    }

    /**
     * Getter for the "RepNodeMultiIntervalPct99" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalPct99() throws SnmpStatusException {
        return RepNodeMultiIntervalPct99;
    }

    /**
     * Getter for the "RepNodeMultiIntervalPct95" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalPct95() throws SnmpStatusException {
        return RepNodeMultiIntervalPct95;
    }

    /**
     * Getter for the "RepNodeMultiIntervalLatAvgFrac" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalLatAvgFrac() throws SnmpStatusException {
        return RepNodeMultiIntervalLatAvgFrac;
    }

    /**
     * Getter for the "RepNodeMultiIntervalLatAvgInt" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalLatAvgInt() throws SnmpStatusException {
        return RepNodeMultiIntervalLatAvgInt;
    }

    /**
     * Getter for the "RepNodeCommitLagThreshold" variable.
     */
    @Override
    public Long getRepNodeCommitLagThreshold() throws SnmpStatusException {
        return RepNodeCommitLagThreshold;
    }

    /**
     * Getter for the "RepNodeMultiIntervalLatAvg" variable.
     */
    @Override
    public String getRepNodeMultiIntervalLatAvg() throws SnmpStatusException {
        return RepNodeMultiIntervalLatAvg;
    }

    /**
     * Getter for the "RepNodeMultiCumulativeTotalRequests" variable.
     */
    @Override
    public Integer getRepNodeMultiCumulativeTotalRequests() throws SnmpStatusException {
        return RepNodeMultiCumulativeTotalRequests;
    }

    /**
     * Getter for the "RepNodeMultiIntervalLatMax" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalLatMax() throws SnmpStatusException {
        return RepNodeMultiIntervalLatMax;
    }

    /**
     * Getter for the "RepNodeMultiIntervalTotalRequests" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalTotalRequests() throws SnmpStatusException {
        return RepNodeMultiIntervalTotalRequests;
    }

    /**
     * Getter for the "RepNodeThroughputFloor" variable.
     */
    @Override
    public Integer getRepNodeThroughputFloor() throws SnmpStatusException {
        return RepNodeThroughputFloor;
    }

    /**
     * Getter for the "RepNodeMultiIntervalLatMin" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalLatMin() throws SnmpStatusException {
        return RepNodeMultiIntervalLatMin;
    }

    /**
     * Getter for the "RepNodeMultiIntervalThroughput" variable.
     */
    @Override
    public Long getRepNodeMultiIntervalThroughput() throws SnmpStatusException {
        return RepNodeMultiIntervalThroughput;
    }

    /**
     * Getter for the "RepNodeMultiIntervalTotalOps" variable.
     */
    @Override
    public Integer getRepNodeMultiIntervalTotalOps() throws SnmpStatusException {
        return RepNodeMultiIntervalTotalOps;
    }

}