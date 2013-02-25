package ch.x42.terye.mk.hbase;

import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JOURNAL;
import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NODES;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jackrabbit.oak.commons.PathUtils;

import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JournalTable;
import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NodeTable;
import ch.x42.terye.mk.hbase.HBaseTableDefinition.Qualifier;

/**
 * This class provides methods that tests can use in order to create custom
 * scenarios for particular test cases. Those methods do not build on the any of
 * the HBaseMicroKernel code and are assumed to be correct. The consistency of
 * the simulated scenario is not verified and left as the caller's
 * responsibility.
 */
public class HBaseMicroKernelTestScenario {

    private HTable nodeTable;
    private HTable journalTable;
    private RevisionIdGenerator revIdGenerator;
    private long revisionId;

    public HBaseMicroKernelTestScenario(HBaseAdmin admin) throws Exception {
        journalTable = HBaseUtils.getOrCreateTable(admin, JOURNAL);
        nodeTable = HBaseUtils.getOrCreateTable(admin, NODES);
        revIdGenerator = new RevisionIdGenerator(new Random().nextInt(65536));
        revisionId = -1;
    }

    private void startCommit() {
        if (revisionId == -1) {
            revisionId = revIdGenerator.getNewId();
        }
    }

    /**
     * Adds a new node at a given path.
     * 
     * @param path the node path
     */
    public void addNode(String path) throws IOException {
        startCommit();
        // add new node
        Put put = new Put(NodeTable.pathToRowKey(path), revisionId);
        put.add(NodeTable.CF_DATA.toBytes(), NodeTable.COL_DELETED.toBytes(),
                revisionId, Bytes.toBytes(false));
        put.add(NodeTable.CF_DATA.toBytes(),
                NodeTable.COL_LAST_REVISION.toBytes(), revisionId,
                Bytes.toBytes(revisionId));
        put.add(NodeTable.CF_DATA.toBytes(),
                NodeTable.COL_CHILD_COUNT.toBytes(), revisionId,
                Bytes.toBytes(0L));
        nodeTable.put(put);
        // increment child node count of parent node
        String parentPath = PathUtils.getParentPath(path);
        put = new Put(NodeTable.pathToRowKey(parentPath), revisionId);
        byte[] bytes = getProperty(parentPath, NodeTable.COL_CHILD_COUNT);
        long cc = Bytes.toLong(bytes) + 1;
        put.add(NodeTable.CF_DATA.toBytes(),
                NodeTable.COL_CHILD_COUNT.toBytes(), revisionId,
                Bytes.toBytes(cc));
        nodeTable.put(put);
    }

    /**
     * Adds a new node at a given path.
     * 
     * @param path the node path
     */
    public void setProperty(String path, Object value) throws IOException {
        startCommit();
        String parentPath = PathUtils.getParentPath(path);
        String name = PathUtils.getName(path);
        Qualifier q = new Qualifier(NodeTable.DATA_PROPERTY_PREFIX, name);
        Put put = new Put(NodeTable.pathToRowKey(parentPath), revisionId);
        put.add(NodeTable.CF_DATA.toBytes(), q.toBytes(), revisionId,
                NodeTable.toBytes(value));
        nodeTable.put(put);
    }

    private byte[] getProperty(String parentPath, Qualifier qualifier)
            throws IOException {
        Get get = new Get(NodeTable.pathToRowKey(parentPath));
        get.addColumn(NodeTable.CF_DATA.toBytes(), qualifier.toBytes());
        return nodeTable.get(get).getValue(NodeTable.CF_DATA.toBytes(),
                qualifier.toBytes());
    }

    /**
     */
    public String commit(String message) throws IOException {
        // add journal entry
        Put put = new Put(Bytes.toBytes(revisionId));
        put.add(JournalTable.CF_DATA.toBytes(),
                JournalTable.COL_COMMITTED.toBytes(), Bytes.toBytes(true));
        put.add(JournalTable.CF_DATA.toBytes(),
                JournalTable.COL_ABORT.toBytes(), Bytes.toBytes(false));
        String msg = message != null ? message : "commit msg for rev "
                + revisionId;
        put.add(JournalTable.CF_DATA.toBytes(),
                JournalTable.COL_MESSAGE.toBytes(), Bytes.toBytes(msg));
        journalTable.put(put);
        String res = String.valueOf(revisionId);
        revisionId = -1;
        return res;
    }

    /**
     */
    public String commit() throws IOException {
        return commit(null);
    }

}
