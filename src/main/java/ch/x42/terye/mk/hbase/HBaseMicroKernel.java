package ch.x42.terye.mk.hbase;

import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JOURNAL;
import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NODES;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.oak.commons.PathUtils;

import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JournalTable;
import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NodeTable;
import ch.x42.terye.mk.hbase.HBaseTableDefinition.Qualifier;

public class HBaseMicroKernel implements MicroKernel {

    /* configuration */

    // the timestamp of the revision ids generated is based on the following
    // date instead of the usual unix epoch:
    // 01.01.2013 00:00:00
    // NEVER EVER CHANGE THIS VALUE
    public static final long EPOCH = 1356998400;
    // max number of retries in case of a concurrent modification
    private static final int MAX_RETRIES = 100;
    // max number of entries in the LRU node cache
    private static final int MAX_CACHE_ENTRIES = 1000;

    private HBaseTableManager tableMgr;
    private Journal journal;
    private NodeCache cache;
    // the machine id associated with this microkernel instance
    private long machineId;
    // the timestamp of the revision id last generated
    private long lastTimestamp;
    // counter to differentiate revision ids generated within the same second
    private int count;

    /**
     * This constructor can be used to explicitely set the machine id. This is
     * useful when concurrently executing multiple microkernels on the same
     * machine.
     */
    public HBaseMicroKernel(HBaseAdmin admin, int machineId) throws Exception {
        tableMgr = new HBaseTableManager(admin, HBaseMicroKernelSchema.TABLES);
        journal = new Journal(tableMgr.create(JOURNAL));
        cache = new NodeCache(MAX_CACHE_ENTRIES);
        if (machineId < 0 || machineId > 65535) {
            throw new IllegalArgumentException("Machine id is out of range");
        }
        this.machineId = (long) machineId;
        this.lastTimestamp = -1;
        this.count = 0;
    }

    public HBaseMicroKernel(HBaseAdmin admin) throws Exception {
        this(admin, 0);
        // generate machine id
        try {
            // get MAC address string
            NetworkInterface network = NetworkInterface
                    .getByInetAddress(InetAddress.getLocalHost());
            byte[] address = network.getHardwareAddress();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < address.length; i++) {
                sb.append(String.format("%02X", address[i]));
            }
            machineId = (long) sb.hashCode();
        } catch (Throwable e) {
            // some error occurred, use a random machine id
            machineId = (long) new Random().nextInt();
        }
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        return String.valueOf(journal.getHeadRevisionId());
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path,
            int depth) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        try {
            // parse revision id
            long revId = journal.getHeadRevisionId();
            if (revisionId != null) {
                try {
                    revId = Long.parseLong(revisionId);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid revision id: "
                            + revisionId);
                }
            }

            // do a filtered prefix scan:
            Scan scan = new Scan();
            scan.setMaxVersions();
            // compute scan range
            String prefix = path
                    + (path.charAt(path.length() - 1) == '/' ? "" : "/");
            byte[] startRow = Bytes.toBytes(prefix);
            byte[] stopRow = startRow.clone();
            // make stop row inclusive
            stopRow[stopRow.length - 1]++;
            scan.setStartRow(startRow);
            scan.setStopRow(stopRow);
            // limit the depth of the scan
            String regex = "^" + Pattern.quote(prefix) + "(([^/])+/){0,"
                    + (depth + 1) + "}$";
            Filter depthFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new RegexStringComparator(regex));
            scan.setFilter(depthFilter);
            Map<String, Result> rows = new LinkedHashMap<String, Result>();
            ResultScanner scanner = tableMgr.get(NODES).getScanner(scan);
            for (Result result : scanner) {
                rows.put(NodeTable.rowKeyToPath(result.getRow()), result);
            }
            scanner.close();

            // parse nodes, create tree, build and return JSON
            Map<String, Node> nodes = parseNodes(rows, revId);
            return Node.toJson(Node.toTree(nodes), depth);
        } catch (Exception e) {
            throw new MicroKernelException("Error while getting nodes", e);
        }
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId,
            String message) throws MicroKernelException {
        try {
            // parse diff to an update object
            Update update = new Update();
            new JsopParser(path, jsonDiff, update.getJsopHandler()).parse();

            int tries = MAX_RETRIES;
            long newRevId;
            Map<String, Node> nodesBefore;
            do {
                if (--tries < 0) {
                    throw new MicroKernelException("Reached retry limit");
                }

                // generate new revision id
                newRevId = generateNewRevisionId();

                // write journal entry for this revision
                Put put = new Put(Bytes.toBytes(newRevId));
                put.add(JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_COMMITTED.toBytes(),
                        Bytes.toBytes(false));
                put.add(JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_ABORT.toBytes(), Bytes.toBytes(false));
                tableMgr.get(JOURNAL).put(put);

                // update journal in order to have the newest possible versions
                // of the node we read before our write
                journal.update();

                // read nodes that are to be written
                nodesBefore = getNodes(update.getModifiedNodes(), null);

                // make sure the update is valid
                validateUpdate(nodesBefore, update);

                // generate update batch and write it to HBase
                List<Row> batch = generateUpdateOps(nodesBefore, update,
                        newRevId);
                Object[] results = tableMgr.get(NODES).batch(batch);
                for (Object result : results) {
                    // a null result means the write operation failed, in which
                    // case we roll back
                    if (result == null) {
                        rollback(update, newRevId);
                        continue;
                    }
                }

                // check for potential concurrent modifications
                if (verifyUpdate(nodesBefore, update, newRevId)) {
                    // commit revision
                    put = new Put(Bytes.toBytes(newRevId));
                    put.add(JournalTable.CF_DATA.toBytes(),
                            JournalTable.COL_COMMITTED.toBytes(),
                            Bytes.toBytes(true));
                    if (tableMgr.get(JOURNAL).checkAndPut(
                            Bytes.toBytes(newRevId),
                            JournalTable.CF_DATA.toBytes(),
                            JournalTable.COL_ABORT.toBytes(),
                            Bytes.toBytes(false), put))
                        // revision has been committed
                        break;
                    else {
                        // our revision has been marked as abort by another
                        // revision, so we roll back
                        rollback(update, newRevId);
                        continue;
                    }
                }
                // there has been a concurrent modification, do a rollback
                rollback(update, newRevId);
            } while (true);

            journal.addRevisionId(newRevId);
            return String.valueOf(newRevId);
        } catch (Exception e) {
            throw new MicroKernelException("Commit failed", e);
        }
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Nonnull
    public String rebase(@Nonnull String branchRevisionId,
            String newBaseRevisionId) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    /* private methods */

    /* helper methods for commit */

    /**
     * This method generates a new revision id. A revision id is a signed (but
     * non-negative) long composed of the concatenation (left-to-right) of
     * following fields:
     * 
     * <pre>
     *  ------------------------------------------------------------------
     * |                              timestamp |     machine_id |  count |
     *  ------------------------------------------------------------------
     *  
     * - timestamp: 5 bytes, [0, 1099511627775]
     * - machine_id: 2 bytes, [0, 65535]
     * - count: 1 byte, [0, 65535]
     * </pre>
     * 
     * The unit of the "timestamp" field is milliseconds and since we only have
     * 4 bytes available, we base it on an artificial epoch constant in order to
     * have more values available. The machine id used for "machine_id" is
     * either set explicitly or generated using the hashcode of the MAC address
     * string. If the same machine commits a revision within the same
     * millisecond, then the "count" field is used in order to create a unique
     * revision id.
     * 
     * @return a new and unique revision id
     */
    private long generateNewRevisionId() {
        // timestamp
        long timestamp = System.currentTimeMillis() - EPOCH;
        timestamp <<= 24;
        // check for overflow
        if (timestamp < 0) {
            // we have use all available time values
            throw new MicroKernelException("Error generating new revision id");
        }

        // machine id
        long machineId = (this.machineId << 16)
                & Long.decode("0x00000000FFFF0000");

        // counter
        if (timestamp == lastTimestamp) {
            count++;
        } else {
            lastTimestamp = timestamp;
            count = 0;
        }
        long count = this.count & Integer.decode("0x000000FF");

        // assemble and return revision id
        return timestamp | machineId | count;
    }

    /**
     * This method validates the changes that are to be committed.
     * 
     * @param nodesBefore the current state of the nodes that will be modified
     * @param update the update to validate
     * @throws MicroKernelException if the update is not valid
     */
    private void validateUpdate(Map<String, Node> nodesBefore, Update update)
            throws MicroKernelException {
        Set<String> parents = new HashSet<String>();
        parents.addAll(nodesBefore.keySet());
        parents.addAll(update.getAddedNodes());
        // verify that all the nodes to be added have a valid parent
        for (String path : update.getAddedNodes()) {
            String parentPath = PathUtils.getParentPath(path);
            if (!parents.contains(parentPath)) {
                throw new MicroKernelException("Cannot add node " + path
                        + ": parent doesn't exist");
            }
            if (nodesBefore.containsKey(path)) {
                throw new MicroKernelException("Cannot add node " + path
                        + ": node already exists");
            }
        }
    }

    private List<Row> generateUpdateOps(Map<String, Node> nodesBefore,
            Update update, long newRevisionId) {
        Map<String, Put> puts = new HashMap<String, Put>();
        Put put;
        // - added nodes
        for (String node : update.getAddedNodes()) {
            put = getPut(node, newRevisionId, puts);
            // child count
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), newRevisionId,
                    Bytes.toBytes(0L));
        }
        // - changed child counts
        for (Entry<String, Long> entry : update.getChangedChildCounts()
                .entrySet()) {
            String node = entry.getKey();
            long childCount;
            if (nodesBefore.containsKey(node)) {
                childCount = nodesBefore.get(node).getChildCount()
                        + entry.getValue();
            } else {
                childCount = entry.getValue();
            }
            put = getPut(node, newRevisionId, puts);
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), newRevisionId,
                    Bytes.toBytes(childCount));
        }
        // - set properties
        for (Entry<String, Object> entry : update.getSetProperties().entrySet()) {
            String parentPath = PathUtils.getParentPath(entry.getKey());
            String name = PathUtils.getName(entry.getKey());
            Object value = entry.getValue();
            byte typePrefix;
            byte[] tmp;
            if (value instanceof String) {
                typePrefix = NodeTable.TYPE_STRING_PREFIX;
                tmp = Bytes.toBytes((String) value);
            } else if (value instanceof Number) {
                typePrefix = NodeTable.TYPE_LONG_PREFIX;
                tmp = Bytes.toBytes(((Number) value).longValue());
            } else if (value instanceof Boolean) {
                typePrefix = NodeTable.TYPE_BOOLEAN_PREFIX;
                tmp = Bytes.toBytes((Boolean) value);
            } else {
                throw new MicroKernelException("Property " + entry.getKey()
                        + " has unknown type " + value.getClass());
            }
            put = getPut(parentPath, newRevisionId, puts);
            Qualifier q = new Qualifier(NodeTable.DATA_PROPERTY_PREFIX, name);
            byte[] bytes = new byte[tmp.length + 1];
            bytes[0] = typePrefix;
            System.arraycopy(tmp, 0, bytes, 1, tmp.length);
            put.add(NodeTable.CF_DATA.toBytes(), q.toBytes(), newRevisionId,
                    bytes);
        }
        // assemble operations
        List<Row> ops = new LinkedList<Row>();
        ops.addAll(puts.values());
        return ops;
    }

    private Put getPut(String path, long revisionId, Map<String, Put> puts) {
        if (!puts.containsKey(path)) {
            Put put = new Put(NodeTable.pathToRowKey(path), revisionId);
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_LAST_REVISION.toBytes(), revisionId,
                    Bytes.toBytes(revisionId));
            puts.put(path, put);
        }
        return puts.get(path);
    }

    /**
     * This method verifies that the update has not conflicted with another
     * concurrent update.
     * 
     * @param nodesBefore the state of the nodes that were written before the
     *            write
     * @param update the update to verify
     * @param revisionId the revision id of the revision the changes were
     *            written in
     * @throws MicroKernelException if there was a conflicting concurrent update
     */
    private boolean verifyUpdate(Map<String, Node> nodesBefore, Update update,
            long revisionId) throws MicroKernelException, IOException {
        Map<String, Result> nodesAfter = getNodeRows(update.getModifiedNodes());
        // loop through all nodes we have written
        for (String path : update.getModifiedNodes()) {
            Result after = nodesAfter.get(path);
            // get "last revision" column
            NavigableMap<Long, byte[]> lastRevCol = after.getMap()
                    .get(NodeTable.CF_DATA.toBytes())
                    .get(NodeTable.COL_LAST_REVISION.toBytes());
            // split column in two parts at our revision
            NavigableMap<Long, byte[]> lastRevColHead = lastRevCol.headMap(
                    revisionId, false);
            NavigableMap<Long, byte[]> lastRevColTail = lastRevCol.tailMap(
                    revisionId, false);
            // check that nobody wrote on top of our uncommitted changes
            if (!lastRevColHead.isEmpty()) {
                // we try to mark those revisions as abort
                boolean success = false;
                for (Long l : lastRevColHead.keySet()) {
                    Put put = new Put(Bytes.toBytes(l));
                    put.add(JournalTable.CF_DATA.toBytes(),
                            JournalTable.COL_ABORT.toBytes(),
                            Bytes.toBytes(true));
                    success = tableMgr.get(JOURNAL).checkAndPut(
                            Bytes.toBytes(l), JournalTable.CF_DATA.toBytes(),
                            JournalTable.COL_COMMITTED.toBytes(),
                            Bytes.toBytes(false), put);
                }
                return success;
            } else {
                // make sure nobody wrote immediately before we did:
                // if the node didn't exist before...
                if (!nodesBefore.containsKey(path)) {
                    // ...then there should be no revision before ours
                    if (!lastRevColTail.isEmpty()) {
                        return false;
                    }
                } else {
                    // ...else the revision before ours must be equal to the one
                    // of the node read before our write
                    Node before = nodesBefore.get(path);
                    if (!lastRevColTail.firstKey().equals(
                            before.getLastRevision())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private void rollback(Update update, long newRevisionId) throws IOException {
        Map<String, Delete> deletes = new HashMap<String, Delete>();
        Delete delete;
        // - rollback added nodes
        for (String node : update.getAddedNodes()) {
            delete = getDelete(node, newRevisionId, deletes);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), newRevisionId);
        }
        // - rollback changed child counts
        for (String node : update.getChangedChildCounts().keySet()) {
            delete = getDelete(node, newRevisionId, deletes);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), newRevisionId);
        }
        // - rollback set properties
        for (String property : update.getSetProperties().keySet()) {
            String parentPath = PathUtils.getParentPath(property);
            String name = PathUtils.getName(property);
            delete = getDelete(parentPath, newRevisionId, deletes);
            Qualifier q = new Qualifier(NodeTable.DATA_PROPERTY_PREFIX, name);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(), q.toBytes(),
                    newRevisionId);
        }
        List<Delete> batch = new LinkedList<Delete>(deletes.values());
        tableMgr.get(NODES).delete(batch);
        // remove journal entry
        delete = new Delete(Bytes.toBytes(newRevisionId));
        tableMgr.get(JOURNAL).delete(delete);
    }

    private Delete getDelete(String path, long revisionId,
            Map<String, Delete> deletes) {
        if (!deletes.containsKey(path)) {
            Delete delete = new Delete(NodeTable.pathToRowKey(path));
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_LAST_REVISION.toBytes(), revisionId);
            deletes.put(path, delete);
        }
        return deletes.get(path);
    }

    /* helper methods for reading the node table */

    private Map<String, Result> getNodeRows(Collection<String> paths)
            throws IOException {
        Map<String, Result> nodes = new LinkedHashMap<String, Result>();
        if (paths.isEmpty()) {
            return nodes;
        }
        List<Get> batch = new LinkedList<Get>();
        for (String path : paths) {
            Get get = new Get(NodeTable.pathToRowKey(path));
            get.setMaxVersions();
            batch.add(get);
        }
        for (Result result : tableMgr.get(NODES).get(batch)) {
            if (!result.isEmpty()) {
                String path = NodeTable.rowKeyToPath(result.getRow());
                nodes.put(path, result);
            }
        }
        return nodes;
    }

    private Map<String, Node> getNodes(Collection<String> paths, Long revisionId)
            throws IOException {
        // get current head revision if revision id not set
        long revId = revisionId == null ? journal.getHeadRevisionId()
                : revisionId;
        Map<String, Node> nodes = new TreeMap<String, Node>();
        List<String> pathsToRead = new LinkedList<String>();
        for (String path : paths) {
            Node node = cache.get(revId, path);
            if (node != null) {
                nodes.put(path, node);
            } else {
                pathsToRead.add(path);
            }
        }
        // XXX: don't get all revisions
        Map<String, Result> rows = getNodeRows(pathsToRead);
        for (Node node : parseNodes(rows, revId).values()) {
            cache.put(revId, node);
            nodes.put(node.getPath(), node);
        }
        return nodes;
    }

    /**
     * This method parses the specified node rows into nodes.
     * 
     * @param results the node rows
     * @return a map mapping paths to the corresponding node
     */
    private Map<String, Node> parseNodes(Map<String, Result> rows,
            long revisionId) throws IOException {
        Map<String, Node> nodes = new LinkedHashMap<String, Node>();
        LinkedList<Long> revisionIds = journal.getRevisionIds();
        // loop through all node rows
        for (Result row : rows.values()) {
            Node node = parseNode(row, revisionId, revisionIds);
            if (node != null) {
                nodes.put(node.getPath(), node);
            }
        }
        return nodes;
    }

    private Node parseNode(Result row, long revisionId,
            LinkedList<Long> revisionIds) {
        // create node
        String path = NodeTable.rowKeyToPath(row.getRow());
        Node node = new Node(path);
        // get the entry set of the column map
        Set<Entry<byte[], NavigableMap<Long, byte[]>>> columnSet = row.getMap()
                .get(NodeTable.CF_DATA.toBytes()).entrySet();
        // get iterator starting at the end of the list
        ListIterator<Long> iterator = revisionIds.listIterator(revisionIds
                .size());
        // skip revision ids that might have been added after 'revisionId'
        while (iterator.hasPrevious()) {
            if (iterator.previous() == revisionId) {
                iterator.next();
                break;
            }
        }
        // replay revisions top bottom
        while (iterator.hasPrevious()) {
            Long revId = iterator.previous();
            // loop through all columns
            Iterator<Entry<byte[], NavigableMap<Long, byte[]>>> colIterator = columnSet
                    .iterator();
            while (colIterator.hasNext()) {
                Entry<byte[], NavigableMap<Long, byte[]>> entry = colIterator
                        .next();
                byte[] colName = entry.getKey();
                NavigableMap<Long, byte[]> colVersions = entry.getValue();
                byte[] value = colVersions.get(revId);
                if (value == null) {
                    // there is no value for the current column at the current
                    // revision, so go to next column
                    continue;
                }
                // we have found a value, thus we are done for this column
                colIterator.remove();
                // handle system properties
                if (colName[0] == NodeTable.SYSTEM_PROPERTY_PREFIX) {
                    if (Arrays.equals(colName,
                            NodeTable.COL_LAST_REVISION.toBytes())) {
                        node.setLastRevision(Bytes.toLong(value));
                    } else if (Arrays.equals(colName,
                            NodeTable.COL_CHILD_COUNT.toBytes())) {
                        node.setChildCount(Bytes.toLong(value));
                    }
                }
                // handle user properties
                else if (colName[0] == NodeTable.DATA_PROPERTY_PREFIX) {
                    // name
                    byte[] tmp = new byte[colName.length - 1];
                    System.arraycopy(colName, 1, tmp, 0, tmp.length);
                    String name = Bytes.toString(tmp);
                    // value
                    Object val;
                    tmp = new byte[value.length - 1];
                    System.arraycopy(value, 1, tmp, 0, value.length - 1);
                    if (value[0] == NodeTable.TYPE_STRING_PREFIX) {
                        val = Bytes.toString(tmp);
                    } else if (value[0] == NodeTable.TYPE_LONG_PREFIX) {
                        val = Bytes.toLong(tmp);
                    } else if (value[0] == NodeTable.TYPE_BOOLEAN_PREFIX) {
                        val = Bytes.toBoolean(tmp);
                    } else {
                        throw new MicroKernelException("Property "
                                + PathUtils.concat(path, name)
                                + " has unknown type prefix " + value[0]);
                    }
                    node.setProperty(name, val);
                }
            }
        }
        return node;
    }

    /**
     * Disposes all of the resources used by this microkernel instance.
     */
    public void dispose() throws IOException {
        dispose(false);
    }

    /**
     * Disposes all of the resources and optionally drops all tables used by
     * this microkernel instance.
     */
    public void dispose(boolean dropTables) throws IOException {
        journal.dispose();
        cache.clear();
        if (dropTables) {
            try {
                tableMgr.dropAllTables();
            } catch (TableNotFoundException e) {
                // nothing to do
            }
        }
        tableMgr.dispose();
    }

}
