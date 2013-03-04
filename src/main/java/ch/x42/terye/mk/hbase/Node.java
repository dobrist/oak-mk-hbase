package ch.x42.terye.mk.hbase;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;

public class Node {

    private String path;
    private long lastRevision;
    private Set<String> children;
    private Map<String, Object> properties;

    public Node(String path) {
        this.path = path;
        this.children = new LinkedHashSet<String>();
        this.properties = new LinkedHashMap<String, Object>();
    }

    public Node(Node node) {
        this.path = node.path;
        this.children = new LinkedHashSet<String>(node.children);
        this.properties = new LinkedHashMap<String, Object>(node.properties);
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getLastRevision() {
        return lastRevision;
    }

    public void setLastRevision(long lastRevision) {
        this.lastRevision = lastRevision;
    }

    public Set<String> getChildren() {
        return children;
    }

    public void setChildren(Set<String> childrenNames) {
        this.children.addAll(childrenNames);
    }

    public void addChild(String name) {
        children.add(name);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperty(String name, Object value) {
        if (value == null) {
            properties.remove(name);
        } else {
            properties.put(name, value);
        }
    }

    /**
     * Generates the JSON representation of a tree of nodes. The validity of the
     * hierarchy is not verified.
     * 
     * @param nodes the nodes to of the tree
     * @param root the path of the root of the tree
     * @param depth the desired depth or null for infinite depth
     * @return the generated JSON string
     */
    public static String toJson(Map<String, Node> nodes, String root,
            Integer depth) {
        JsopBuilder builder = new JsopBuilder();
        toJson(nodes, depth, builder, root, true);
        return builder.toString();
    }

    private static void toJson(Map<String, Node> nodes, Integer depth,
            JsopBuilder builder, String path, boolean excludeRoot) {
        if (!excludeRoot) {
            builder.key(PathUtils.getName(path));
        }
        builder.object();
        if (depth != null && depth < 0) {
            builder.endObject();
            return;
        }
        Node node = nodes.get(path);
        // virtual properties
        builder.key(":childNodeCount").value(node.getChildren().size());
        // properties
        for (Entry<String, Object> entry : node.getProperties().entrySet()) {
            builder.key(entry.getKey());
            Object value = entry.getValue();
            String encodedValue = value.toString();
            if (value instanceof String) {
                encodedValue = JsopBuilder.encode(value.toString());
            }
            builder.encodedValue(encodedValue);
        }
        // child nodes
        for (String child : node.getChildren()) {
            toJson(nodes, depth == null ? null : depth - 1, builder,
                    PathUtils.concat(path, child), false);
        }
        builder.endObject();
    }

}
