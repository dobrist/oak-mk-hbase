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

import javax.annotation.Nonnull;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.commons.PathUtils;

import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JournalTable;
import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NodeTable;

/**
 * Microkernel prototype implementation using Apache HBase.
 */
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
            return node.getChildren().size();
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
            // get journal snapshot
            long revId = getRevisionId(revisionId);
            LinkedList<Long> journal = this.journal.get(revId);

            // fetch root node
            List<String> paths = new LinkedList<String>();
            paths.add(path);
            Node root = getNodes(paths, journal).get(path);
            if (root == null) {
                return null;
            }
            Map<String, Node> nodes = new HashMap<String, Node>();
            nodes.put(path, root);

            // fetch further levels (one HBase call per level)
            Set<Node> currentNodes = new HashSet<Node>();
            currentNodes.add(root);
            for (int i = 1; i <= depth; i++) {
                paths.clear();
                // collect children paths of the nodes of the current level
                for (Node node : currentNodes) {
                    for (String child : node.getChildren()) {
                        paths.add(PathUtils.concat(node.getPath(), child));
                    }
                }
                // get the children
                Map<String, Node> ns = getNodes(paths, journal);
                nodes.putAll(ns);
                currentNodes.clear();
                currentNodes.addAll(ns.values());
            }

            // create and return JSON
            if (nodes.isEmpty()) {
                return null;
            }
            return Node.toJson(nodes, path, depth);
        } catch (Exception e) {
            throw new MicroKernelException("Error while getting nodes", e);
        }
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId,
            String message) throws MicroKernelException {
        try {
            // create update object
            HBaseUpdate update = new HBaseUpdate(path, jsonDiff);
            // number of tries
            int tries = 0;
            // id of new revision
            long newRevId = -1;
            // nodes read before writing the update
            Map<String, Node> nodesBefore;
            // exponentially increasing timeout after unsuccessful tries
            double backoffTimeout = 1;
            do {
                // rollback previous unsuccessful try
                if (tries > 0) {
                    journal.unlock();
                    List<Delete> batch = update.unapply(newRevId);
                    nodeTable.delete(batch);
                }
                // check if retry limit has been reached
                if (++tries > MAX_RETRIES) {
                    throw new MicroKernelException("Reached retry limit");
                }
                // backoff
                if (tries > 2) {
                    Thread.sleep((long) backoffTimeout);
                    backoffTimeout *= 1.5;
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

                // generate update batch and write it to HBase
                List<Put> batch = update.apply(nodesBefore, newRevId);
                nodeTable.put(batch);

                // check for potential concurrent modifications
                if (verifyUpdate(nodesBefore, journal, update, newRevId)) {
                    // before committing, check if the execution time of this
                    // try is within the bounds of the grace period defined in
                    // the journal
                    if (System.currentTimeMillis() - start + 50 > Journal.GRACE_PERIOD) {
                        // rollback as a safety measure
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
                        continue;
                    }
                }
                // there has been a concurrent modification, do a rollback
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
            LinkedList<Long> revisionIds, HBaseUpdate update, long newRevisionId)
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

    /**
     * Creates in-memory representations of the nodes that have been modified and
     * puts them into the cache.
     */
    private void cacheNodes(Map<String, Node> nodesBefore, HBaseUpdate update,
            long newRevisionId) {
    	// XXX: implement
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
                            NodeTable.COL_CHILDREN.toBytes())) {
                        node.setChildren(NodeTable.deserializeChildren(Bytes
                                .toString(value)));
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
