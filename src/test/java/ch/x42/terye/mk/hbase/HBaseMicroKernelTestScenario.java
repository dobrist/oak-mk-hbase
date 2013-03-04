package ch.x42.terye.mk.hbase;

import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JOURNAL;
import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NODES;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

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
 * scenarios for setting up test cases. The methods put HBase into a known state
 * without using any of the HBaseMicroKernel methods. The methods of this class
 * are assumed to be correct and the consistency of the scenario is left as the
 * caller's responsibility.
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
     * Adds a new node.
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
        // add child to list of children
        bytes = getProperty(parentPath, NodeTable.COL_CHILDREN);
        Set<String> children = NodeTable.deserializeChildren(Bytes
                .toString(bytes));
        children.add(PathUtils.getName(path));
        put.add(NodeTable.CF_DATA.toBytes(), NodeTable.COL_CHILDREN.toBytes(),
                revisionId,
                Bytes.toBytes(NodeTable.serializeChildren(children)));
        nodeTable.put(put);
    }

    /**
     * Removes an existing node.
     * 
     * @param path the node path
     */
    public void removeNode(String path) throws IOException {
        startCommit();
        // mark node as deleted
        Put put = new Put(NodeTable.pathToRowKey(path), revisionId);
        put.add(NodeTable.CF_DATA.toBytes(), NodeTable.COL_DELETED.toBytes(),
                revisionId, Bytes.toBytes(true));
        nodeTable.put(put);
        // decrement child node count of parent node
        String parentPath = PathUtils.getParentPath(path);
        put = new Put(NodeTable.pathToRowKey(parentPath), revisionId);
        byte[] bytes = getProperty(parentPath, NodeTable.COL_CHILD_COUNT);
        long cc = Bytes.toLong(bytes) - 1;
        put.add(NodeTable.CF_DATA.toBytes(),
                NodeTable.COL_CHILD_COUNT.toBytes(), revisionId,
                Bytes.toBytes(cc));
        // delete child from list of children
        bytes = getProperty(parentPath, NodeTable.COL_CHILDREN);
        Set<String> children = NodeTable.deserializeChildren(Bytes
                .toString(bytes));
        children.remove(PathUtils.getName(path));
        put.add(NodeTable.CF_DATA.toBytes(), NodeTable.COL_CHILDREN.toBytes(),
                revisionId,
                Bytes.toBytes(NodeTable.serializeChildren(children)));
        nodeTable.put(put);
    }

    /**
     * Sets a property to a specific value.
     * 
     * @param path the property path
     * @param value the non-null value
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
     * Commits the scenario.
     * 
     * @param message the commit message
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
     * Commits the scenario with a default message.
     */
    public String commit() throws IOException {
        return commit(null);
    }

}
