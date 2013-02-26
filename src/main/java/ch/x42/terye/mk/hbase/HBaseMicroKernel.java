package ch.x42.terye.mk.hbase;

import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JOURNAL;
import static ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NODES;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.hadoop.hbase.client.HTable;
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

    // read journal table every so many milliseconds
    public static final int JOURNAL_UPDATE_TIMEOUT = 1500;
    // max number of retries in case of a concurrent modification
    public static final int MAX_RETRIES = 100;
    // max number of entries in the LRU node cache
    public static final int MAX_CACHE_ENTRIES = 1000;

    private HBaseAdmin admin;
    private HTable journalTable;
    private HTable nodeTable;

    private RevisionIdGenerator revIdGenerator;
    private Journal journal;
    private NodeCache cache;

    /**
     * This constructor can be used to explicitly set the machine id. This is
     * useful when concurrently executing multiple microkernels on the same
     * machine.
     */
    public HBaseMicroKernel(HBaseAdmin admin, Integer machineId)
            throws Exception {
        this.admin = admin;
        journalTable = HBaseUtils.getOrCreateTable(admin, JOURNAL);
        nodeTable = HBaseUtils.getOrCreateTable(admin, NODES);

        int mid = machineId == null ? getMachineId() : machineId;
        revIdGenerator = new RevisionIdGenerator(mid);
        journal = new Journal(HBaseUtils.getOrCreateTable(admin, JOURNAL),
                JOURNAL_UPDATE_TIMEOUT);
        cache = new NodeCache(MAX_CACHE_ENTRIES);
    }

    public HBaseMicroKernel(HBaseAdmin admin, int machineId) throws Exception {
        this(admin, new Integer(machineId));
    }

    public HBaseMicroKernel(HBaseAdmin admin) throws Exception {
        this(admin, null);
    }

    private int getMachineId() {
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
            return sb.hashCode() & Integer.decode("0x0000FFFF");
        } catch (Throwable e) {
            // some error occurred, use a random machine id
            return new Random().nextInt(65536);
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
        try {
            return getNode(path, revisionId) != null;
        } catch (Exception e) {
            throw new MicroKernelException("Error testing existence of node "
                    + path, e);
        }
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        try {
            Node node = getNode(path, revisionId);
            if (node == null) {
                String s = revisionId == null ? "head revision" : "revision "
                        + revisionId;
                throw new MicroKernelException("Node " + path
                        + " doesn't exist in " + s);
            }
            return node.getChildCount();
        } catch (Exception e) {
            throw new MicroKernelException("Error getting child count of node "
                    + path, e);
        }
    }

    @Override
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        try {
            // get journal
            long revId = getRevisionId(revisionId);
            LinkedList<Long> journal = this.journal.get(revId);

            // do a filtered prefix scan:
            Scan scan = new Scan();
            // get all versions of the columns
            scan.setMaxVersions();
            // set time range according to the journal
            scan.setTimeRange(0L, Collections.max(journal) + 1);
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
            ResultScanner scanner = nodeTable.getScanner(scan);
            for (Result result : scanner) {
                rows.put(NodeTable.rowKeyToPath(result.getRow()), result);
            }
            scanner.close();

            // parse nodes
            Map<String, Node> nodes = parseNodes(rows, journal);
            if (nodes.isEmpty()) {
                return null;
            }
            // create tree, build and return JSON
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

            int tries = 0;
            long newRevId;
            Map<String, Node> nodesBefore;
            do {
                if (++tries > MAX_RETRIES) {
                    throw new MicroKernelException("Reached retry limit");
                }

                // generate new revision id
                long start = System.currentTimeMillis();
                newRevId = revIdGenerator.getNewId();

                // write journal entry for this revision
                Put put = new Put(Bytes.toBytes(newRevId));
                put.add(JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_COMMITTED.toBytes(),
                        Bytes.toBytes(false));
                put.add(JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_ABORT.toBytes(), Bytes.toBytes(false));
                put.add(JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_MESSAGE.toBytes(),
                        Bytes.toBytes(message));
                journalTable.put(put);

                // update journal on first retry
                if (tries > 1) {
                    journal.update();
                }
                journal.lock();
                LinkedList<Long> journal = this.journal.get();

                // read nodes that are to be written
                nodesBefore = getNodes(update.getModifiedNodes(), journal);

                // make sure the update is valid
                validateUpdate(nodesBefore, update);

                // generate update batch and write it to HBase
                List<Row> batch = generateUpdateOps(nodesBefore, update,
                        newRevId);
                Object[] results = nodeTable.batch(batch);
                for (Object result : results) {
                    // a null result means the write operation failed, in which
                    // case we roll back
                    if (result == null) {
                        rollback(update, newRevId);
                        continue;
                    }
                }

                // check for potential concurrent modifications
                if (verifyUpdate(nodesBefore, journal, update, newRevId)) {
                    // before committing, check if the execution time of this
                    // try is within the bounds of the grace period defined in
                    // the journal
                    if (System.currentTimeMillis() - start + 50 > Journal.GRACE_PERIOD) {
                        // rollback as a safety measure
                        rollback(update, newRevId);
                        continue;
                    }
                    // commit revision
                    put = new Put(Bytes.toBytes(newRevId));
                    put.add(JournalTable.CF_DATA.toBytes(),
                            JournalTable.COL_COMMITTED.toBytes(),
                            Bytes.toBytes(true));
                    if (journalTable.checkAndPut(Bytes.toBytes(newRevId),
                            JournalTable.CF_DATA.toBytes(),
                            JournalTable.COL_ABORT.toBytes(),
                            Bytes.toBytes(false), put)) {
                        // revision has been committed
                        break;
                    } else {
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
            journal.unlock();
            cacheNodes(nodesBefore, update, newRevId);
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

    /**
     * Parses and returns the specified revision id. If the parameter is null,
     * the method returns the current head revision id.
     * 
     * @param revisionId the revision id to be parsed or null for getting the
     *            current head revision id
     * @return the parse revision id or the current head revision id
     */
    private long getRevisionId(String revisionId) {
        long revId;
        if (revisionId != null) {
            // parse revision id
            try {
                revId = Long.parseLong(revisionId);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid revision id: "
                        + revisionId);
            }
        } else {
            // use current head revision id
            revId = journal.getHeadRevisionId();
        }
        return revId;
    }

    /* helper methods for commit */

    /**
     * This method validates the changes that are to be committed.
     * 
     * @param nodesBefore the current state of the nodes that will be modified
     * @param update the update to validate
     * @throws MicroKernelException if the update is not valid
     */
    private void validateUpdate(Map<String, Node> nodesBefore, Update update)
            throws MicroKernelException {
        // assemble nodes that already exist or have been added in this update
        Set<String> nodes = new HashSet<String>();
        nodes.addAll(nodesBefore.keySet());
        nodes.addAll(update.getAddedNodes());
        // verify that all the nodes to be added have a valid parent...
        for (String path : update.getAddedNodes()) {
            String parentPath = PathUtils.getParentPath(path);
            if (!nodes.contains(parentPath)) {
                throw new MicroKernelException("Cannot add node " + path
                        + ": parent doesn't exist");
            }
            // ...and don't exist yet
            if (nodesBefore.containsKey(path)) {
                throw new MicroKernelException("Cannot add node " + path
                        + ": node already exists");
            }
        }
        // verify that all the nodes to be deleted exist
        for (String path : update.getDeletedNodes()) {
            if (!nodes.contains(path)) {
                throw new MicroKernelException("Cannot delete " + path
                        + ": node doesn't exist");
            }
        }
        // verify that properties to be set have a valid parent
        for (String path : update.getSetProperties().keySet()) {
            String parentPath = PathUtils.getParentPath(path);
            if (!nodes.contains(parentPath)) {
                throw new MicroKernelException("Cannot set property " + path
                        + ": parent doesn't exist");
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
            // don't mark as deleted
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_DELETED.toBytes(), newRevisionId,
                    Bytes.toBytes(false));
            // child count
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), newRevisionId,
                    Bytes.toBytes(0L));
        }
        // - deleted nodes
        for (String node : update.getDeletedNodes()) {
            put = getPut(node, newRevisionId, puts);
            // mark as deleted
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_DELETED.toBytes(), newRevisionId,
                    Bytes.toBytes(true));
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
            byte[] bytes;
            if (value == null) {
                // mark property as deleted
                bytes = NodeTable.DELETE_MARKER;
            } else {
                // convert value to bytes
                bytes = NodeTable.toBytes(value);
            }
            put = getPut(parentPath, newRevisionId, puts);
            Qualifier q = new Qualifier(NodeTable.DATA_PROPERTY_PREFIX, name);
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
     * This method checks if the update has conflicted with another concurrent
     * update. In this case the microkernel with the smalles revision id passes
     * while the other revisions get aborted and have to retry
     * 
     * @param nodesBefore the state of the nodes that were written before the
     *            write
     * @param revisionIds the revision ids used to parse 'nodesBefore'
     * @param update the update to verify
     * @param newRevisionId the revision id of the new revision the changes were
     *            written in
     * @throws MicroKernelException if there was a conflicting concurrent update
     */
    private boolean verifyUpdate(Map<String, Node> nodesBefore,
            LinkedList<Long> revisionIds, Update update, long newRevisionId)
            throws MicroKernelException, IOException {
        Map<String, Result> nodesAfter = getNodeRows(update.getModifiedNodes(),
                null);
        // loop through all nodes we have written
        for (String path : update.getModifiedNodes()) {
            Result after = nodesAfter.get(path);
            // get all revisions that have written this node
            Set<Long> lastRevisions = after.getMap()
                    .get(NodeTable.CF_DATA.toBytes())
                    .get(NodeTable.COL_LAST_REVISION.toBytes()).keySet();
            // remove the ones that were known at the time 'beforeNodes' were
            // read
            lastRevisions.removeAll(revisionIds);
            // remove the id of the new revision we're trying to commit
            lastRevisions.remove(newRevisionId);
            // if no revision ids are left..
            if (lastRevisions.isEmpty()) {
                // ...then nobody came in between and we continue
                continue;
            }
            // check if we're the smallest revision id...
            for (Long id : lastRevisions) {
                if (id < newRevisionId) {
                    // we're not the smallest, thus we yield and retry
                    return false;
                }
            }
            // we're the smallest, thus we try to mark the other revisions as
            // abort
            for (Long id : lastRevisions) {
                Put put = new Put(Bytes.toBytes(id));
                put.add(JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_ABORT.toBytes(), Bytes.toBytes(true));
                if (!journalTable.checkAndPut(Bytes.toBytes(id),
                        JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_COMMITTED.toBytes(),
                        Bytes.toBytes(false), put)) {
                    // a revision we tried to mark abort got through, so we stop
                    // and retry
                    return false;
                }
            }
        }
        // success
        return true;
    }

    private void rollback(Update update, long newRevisionId) throws IOException {
        journal.unlock();
        Map<String, Delete> deletes = new HashMap<String, Delete>();
        Delete delete;
        // - rollback added nodes
        for (String node : update.getAddedNodes()) {
            delete = getDelete(node, newRevisionId, deletes);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), newRevisionId);
        }
        // - rollback deleted nodes
        for (String node : update.getDeletedNodes()) {
            delete = getDelete(node, newRevisionId, deletes);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_DELETED.toBytes(), newRevisionId);
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
        nodeTable.delete(batch);
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

    /**
     * Create in-memory representations of the node that have been written and
     * puts them into the cache.
     */
    private void cacheNodes(Map<String, Node> nodesBefore, Update update,
            long newRevisionId) {
        // construct nodes to be cached from 'nodesBefore' and 'update'
        Map<String, Node> nodes = new HashMap<String, Node>();
        // - added nodes
        for (String path : update.getAddedNodes()) {
            Node node = new Node(path);
            nodes.put(path, node);
        }
        // - changed child counts
        for (Entry<String, Long> entry : update.getChangedChildCounts()
                .entrySet()) {
            String path = entry.getKey();
            Node before = nodesBefore.get(path);
            Node node = nodes.containsKey(path) ? nodes.get(path) : new Node(
                    before);
            node.setChildCount(node.getChildCount() + entry.getValue());
        }
        // - set properties
        for (Entry<String, Object> entry : update.getSetProperties().entrySet()) {
            String parentPath = PathUtils.getParentPath(entry.getKey());
            Node before = nodesBefore.get(parentPath);
            Node node = nodes.containsKey(parentPath) ? nodes.get(parentPath)
                    : new Node(before);
            node.setProperty(PathUtils.getName(entry.getKey()),
                    entry.getValue());
        }
        // cache nodes
        for (Node node : nodes.values()) {
            node.setLastRevision(newRevisionId);
            cache.put(newRevisionId, node);
        }
    }

    /* helper methods for reading the node table */

    private Map<String, Result> getNodeRows(Collection<String> paths,
            LinkedList<Long> journal) throws IOException {
        Map<String, Result> nodes = new LinkedHashMap<String, Result>();
        if (paths.isEmpty()) {
            return nodes;
        }
        List<Get> batch = new LinkedList<Get>();
        Long max = journal == null ? null : Collections.max(journal);
        for (String path : paths) {
            Get get = new Get(NodeTable.pathToRowKey(path));
            if (max != null) {
                get.setTimeRange(0L, max + 1);
            }
            get.setMaxVersions();
            batch.add(get);
        }
        for (Result result : nodeTable.get(batch)) {
            if (!result.isEmpty()) {
                String path = NodeTable.rowKeyToPath(result.getRow());
                nodes.put(path, result);
            }
        }
        return nodes;
    }

    private Map<String, Node> getNodes(Collection<String> paths,
            LinkedList<Long> journal) throws IOException {
        long revId = journal.getLast();
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
        Map<String, Result> rows = getNodeRows(pathsToRead, journal);
        for (Node node : parseNodes(rows, journal).values()) {
            cache.put(revId, node);
            nodes.put(node.getPath(), node);
        }
        return nodes;
    }

    private Node getNode(String path, String revisionId) throws IOException {
        // get journal
        long revId = getRevisionId(revisionId);
        LinkedList<Long> journal = this.journal.get(revId);
        // read and return node
        List<String> paths = new LinkedList<String>();
        paths.add(path);
        return getNodes(paths, journal).get(path);
    }

    /**
     * This method parses the specified node rows into nodes.
     * 
     * @param results the node rows
     * @return a map mapping paths to the corresponding node
     */
    private Map<String, Node> parseNodes(Map<String, Result> rows,
            LinkedList<Long> journal) throws IOException {
        Map<String, Node> nodes = new LinkedHashMap<String, Node>();
        // loop through all node rows
        for (Result row : rows.values()) {
            Node node = parseNode(row, journal);
            if (node != null) {
                nodes.put(node.getPath(), node);
            }
        }
        return nodes;
    }

    private Node parseNode(Result row, LinkedList<Long> journal) {
        // create node
        String path = NodeTable.rowKeyToPath(row.getRow());
        Node node = null;
        // get the entry set of the column map
        Set<Entry<byte[], NavigableMap<Long, byte[]>>> columnSet = row.getMap()
                .get(NodeTable.CF_DATA.toBytes()).entrySet();
        // get iterator starting at the end of the journal
        ListIterator<Long> iterator = journal.listIterator(journal.size());
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
                // check if this column has been marked deleted
                if (Arrays.equals(value, NodeTable.DELETE_MARKER)) {
                    continue;
                }
                // create node if it hasn't been created yet
                if (node == null) {
                    node = new Node(path);
                }
                // handle system properties
                if (colName[0] == NodeTable.SYSTEM_PROPERTY_PREFIX) {
                    if (Arrays.equals(colName, NodeTable.COL_DELETED.toBytes())
                            && Arrays.equals(value, Bytes.toBytes(true))) {
                        // node is marked as deleted
                        return null;
                    } else if (Arrays.equals(colName,
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
                    Object val = NodeTable.fromBytes(value);
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
        journalTable.close();
        nodeTable.close();
        if (dropTables) {
            try {
                HBaseUtils.dropAllTables(admin);
            } catch (TableNotFoundException e) {
                // nothing to do
            }
        }
        admin.close();
    }

}
