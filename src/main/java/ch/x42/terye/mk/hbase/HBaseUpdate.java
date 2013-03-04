package ch.x42.terye.mk.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mongomk.impl.json.DefaultJsopHandler;
import org.apache.jackrabbit.mongomk.impl.json.JsopParser;
import org.apache.jackrabbit.oak.commons.PathUtils;

import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.NodeTable;
import ch.x42.terye.mk.hbase.HBaseTableDefinition.Qualifier;

/**
 * This class represents an update to be written to HBase. It is used to
 * validate the update and to generate HBase operations that reflect or undo the
 * update.
 */
public class HBaseUpdate {

    private Set<String> modifiedNodes;
    private Set<String> addedNodes;
    private Set<String> deletedNodes;
    private Map<String, Long> changedChildCounts;
    private Map<String, Object> setProperties;

    private Map<String, Put> puts;
    private Map<String, Delete> deletes;

    public HBaseUpdate(String path, String jsonDiff) throws Exception {
        modifiedNodes = new TreeSet<String>();
        addedNodes = new TreeSet<String>();
        deletedNodes = new TreeSet<String>();
        changedChildCounts = new LinkedHashMap<String, Long>();
        setProperties = new HashMap<String, Object>();

        // parse diff
        new JsopParser(path, jsonDiff, new JsopHandler()).parse();
    }

    public Set<String> getModifiedNodes() {
        return modifiedNodes;
    }

    public Set<String> getAddedNodes() {
        return addedNodes;
    }

    public Set<String> getDeletedNodes() {
        return deletedNodes;
    }

    public Map<String, Long> getChangedChildCounts() {
        return changedChildCounts;
    }

    public Map<String, Object> getSetProperties() {
        return setProperties;
    }

    /**
     * Validates this updates when applied on the specified set of base nodes.
     * 
     * @param nodes the base nodes this update is based on
     * @throws MicroKernelException if the update is not valid
     */
    private void validate(Map<String, Node> nodes) throws MicroKernelException {
        // assemble nodes that already exist or have been added in this update
        Set<String> allNodes = new HashSet<String>();
        allNodes.addAll(nodes.keySet());
        allNodes.addAll(addedNodes);
        // verify that all the nodes to be added have a valid parent...
        for (String path : addedNodes) {
            String parentPath = PathUtils.getParentPath(path);
            if (!allNodes.contains(parentPath)) {
                throw new MicroKernelException("Cannot add node " + path
                        + ": parent doesn't exist");
            }
            // ...and don't exist yet
            if (nodes.containsKey(path)) {
                throw new MicroKernelException("Cannot add node " + path
                        + ": node already exists");
            }
        }
        // verify that all the nodes to be deleted exist
        for (String path : deletedNodes) {
            if (!allNodes.contains(path)) {
                throw new MicroKernelException("Cannot delete " + path
                        + ": node doesn't exist");
            }
        }
        // verify that properties to be set have a valid parent
        for (String path : setProperties.keySet()) {
            String parentPath = PathUtils.getParentPath(path);
            if (!allNodes.contains(parentPath)) {
                throw new MicroKernelException("Cannot set property " + path
                        + ": parent doesn't exist");
            }
        }
    }

    /**
     * Validates and applies this update on top of the specified set of base
     * nodes and returns a list of puts reflecting this update.
     * 
     * @param nodes the base nodes to apply this update on
     * @param revisionId the id of the revision this update is associated with
     * @return a list of puts reflecting the update
     */
    public List<Put> apply(Map<String, Node> nodes, long revisionId) {
        validate(nodes);
        puts = new HashMap<String, Put>();
        Put put;
        // - added nodes
        for (String node : addedNodes) {
            put = getPut(node, revisionId);
            // don't mark as deleted
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_DELETED.toBytes(), revisionId,
                    Bytes.toBytes(false));
            // child count
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), revisionId,
                    Bytes.toBytes(0L));
        }
        // - deleted nodes
        for (String node : deletedNodes) {
            put = getPut(node, revisionId);
            // mark as deleted
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_DELETED.toBytes(), revisionId,
                    Bytes.toBytes(true));
        }
        // - changed child counts
        for (Entry<String, Long> entry : changedChildCounts.entrySet()) {
            String node = entry.getKey();
            long childCount;
            if (nodes.containsKey(node)) {
                childCount = nodes.get(node).getChildCount() + entry.getValue();
            } else {
                childCount = entry.getValue();
            }
            put = getPut(node, revisionId);
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), revisionId,
                    Bytes.toBytes(childCount));
        }
        // - set properties
        for (Entry<String, Object> entry : setProperties.entrySet()) {
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
            put = getPut(parentPath, revisionId);
            Qualifier q = new Qualifier(NodeTable.DATA_PROPERTY_PREFIX, name);
            put.add(NodeTable.CF_DATA.toBytes(), q.toBytes(), revisionId, bytes);
        }
        return new LinkedList<Put>(puts.values());
    }

    private Put getPut(String path, long revisionId) {
        if (!puts.containsKey(path)) {
            Put put = new Put(NodeTable.pathToRowKey(path), revisionId);
            put.add(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_LAST_REVISION.toBytes(), revisionId,
                    Bytes.toBytes(revisionId));
            puts.put(path, put);
        }
        return puts.get(path);
    }

    public List<Delete> unapply(long revisionId) throws IOException {
        deletes = new HashMap<String, Delete>();
        Delete delete;
        // - rollback added nodes
        for (String node : addedNodes) {
            delete = getDelete(node, revisionId);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), revisionId);
        }
        // - rollback deleted nodes
        for (String node : deletedNodes) {
            delete = getDelete(node, revisionId);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_DELETED.toBytes(), revisionId);
        }
        // - rollback changed child counts
        for (String node : changedChildCounts.keySet()) {
            delete = getDelete(node, revisionId);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_CHILD_COUNT.toBytes(), revisionId);
        }
        // - rollback set properties
        for (String property : setProperties.keySet()) {
            String parentPath = PathUtils.getParentPath(property);
            String name = PathUtils.getName(property);
            delete = getDelete(parentPath, revisionId);
            Qualifier q = new Qualifier(NodeTable.DATA_PROPERTY_PREFIX, name);
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(), q.toBytes(),
                    revisionId);
        }
        return new LinkedList<Delete>(deletes.values());
    }

    private Delete getDelete(String path, long revisionId) {
        if (!deletes.containsKey(path)) {
            Delete delete = new Delete(NodeTable.pathToRowKey(path));
            delete.deleteColumn(NodeTable.CF_DATA.toBytes(),
                    NodeTable.COL_LAST_REVISION.toBytes(), revisionId);
            deletes.put(path, delete);
        }
        return deletes.get(path);
    }

    private class JsopHandler extends DefaultJsopHandler {

        @Override
        public void nodeAdded(String parentPath, String name) {
            String path = PathUtils.concat(parentPath, name);
            addedNodes.add(path);
            modifiedNodes.add(path);
            changeChildCount(PathUtils.getParentPath(path), true);
        }

        @Override
        public void propertySet(String path, String key, Object value,
                String rawValue) {
            String fullPath = PathUtils.concat(path, key);
            setProperties.put(fullPath, value);
            modifiedNodes.add(PathUtils.getParentPath(fullPath));
        }

        @Override
        public void nodeRemoved(String parentPath, String name) {
            String path = PathUtils.concat(parentPath, name);
            deletedNodes.add(path);
            modifiedNodes.add(path);
            changeChildCount(PathUtils.getParentPath(path), false);
        }

        private void changeChildCount(String path, boolean increment) {
            if (!changedChildCounts.containsKey(path)) {
                changedChildCounts.put(path, 0L);
            }
            Long count = changedChildCounts.get(path);
            if (increment) {
                changedChildCounts.put(path, count + 1);
            } else {
                changedChildCounts.put(path, count - 1);
            }
            modifiedNodes.add(path);
        }

    }

}
