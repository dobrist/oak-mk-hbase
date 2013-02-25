package ch.x42.terye.mk.hbase;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * This class defines the HBase schema (i.e. the tables and their schemas) used
 * by the HBase microkernel and provides schema-related helper methods.
 */
public class HBaseMicroKernelSchema {

    public static final class NodeTable extends HBaseTableDefinition {

        // table name
        public static final String TABLE_NAME = "nodes";

        // column families
        public static final Qualifier CF_DATA = new Qualifier("data");
        private static final Qualifier[] COLUMN_FAMILIES = new Qualifier[] {
            CF_DATA
        };

        // column prefixes
        public static final byte SYSTEM_PROPERTY_PREFIX = (byte) 0;
        public static final byte DATA_PROPERTY_PREFIX = (byte) 1;

        // data type prefixes
        public static final byte TYPE_STRING_PREFIX = (byte) 1;
        public static final byte TYPE_LONG_PREFIX = (byte) 2;
        public static final byte TYPE_BOOLEAN_PREFIX = (byte) 3;

        // delete marker
        public static final byte[] DELETE_MARKER = new byte[] {
            (byte) 0
        };

        // columns
        public static final Qualifier COL_DELETED = new Qualifier(
                SYSTEM_PROPERTY_PREFIX, "deleted");
        public static final Qualifier COL_LAST_REVISION = new Qualifier(
                SYSTEM_PROPERTY_PREFIX, "lastRevision");
        public static final Qualifier COL_CHILD_COUNT = new Qualifier(
                SYSTEM_PROPERTY_PREFIX, "childCount");

        // initial content
        private static final List<KeyValue[]> ROWS = new LinkedList<KeyValue[]>();
        static {
            // root node
            long revId = 0L;
            byte[] rowKey = Bytes.toBytes("/");
            KeyValue[] row = {
                    new KeyValue(rowKey, CF_DATA.toBytes(),
                            COL_DELETED.toBytes(), revId, Bytes.toBytes(false)),
                    new KeyValue(rowKey, CF_DATA.toBytes(),
                            COL_LAST_REVISION.toBytes(), revId,
                            Bytes.toBytes(revId)),
                    new KeyValue(rowKey, CF_DATA.toBytes(),
                            COL_CHILD_COUNT.toBytes(), revId, Bytes.toBytes(0L))
            };
            ROWS.add(row);
        };

        private NodeTable() {
            super(TABLE_NAME, COLUMN_FAMILIES, ROWS, Integer.MAX_VALUE);
        }

        public static String pathToRowKeyString(String path) {
            // add trailing slash to path (simplifies prefix scan)
            return PathUtils.denotesRoot(path) ? path : path + "/";
        }

        public static byte[] pathToRowKey(String path) {
            return Bytes.toBytes(pathToRowKeyString(path));
        }

        public static String rowKeyToPath(byte[] rowKey) {
            String rowKeyStr = Bytes.toString(rowKey);
            return PathUtils.denotesRoot(rowKeyStr) ? rowKeyStr : rowKeyStr
                    .substring(0, rowKeyStr.length() - 1);
        }

        public static byte[] toBytes(Object value) {
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
                throw new MicroKernelException("Unsupported value type: "
                        + value.getClass());
            }
            byte[] bytes = new byte[tmp.length + 1];
            bytes[0] = typePrefix;
            System.arraycopy(tmp, 0, bytes, 1, tmp.length);
            return bytes;
        }

        public static Object fromBytes(byte[] value) {
            Object val;
            byte[] tmp = new byte[value.length - 1];
            System.arraycopy(value, 1, tmp, 0, value.length - 1);
            if (value[0] == NodeTable.TYPE_STRING_PREFIX) {
                val = Bytes.toString(tmp);
            } else if (value[0] == NodeTable.TYPE_LONG_PREFIX) {
                val = Bytes.toLong(tmp);
            } else if (value[0] == NodeTable.TYPE_BOOLEAN_PREFIX) {
                val = Bytes.toBoolean(tmp);
            } else {
                throw new MicroKernelException("Unsupported type prefix: "
                        + value[0]);
            }
            return val;
        }

    }

    public static final class JournalTable extends HBaseTableDefinition {

        // table name
        public static final String TABLE_NAME = "journal";

        // column families
        public static final Qualifier CF_DATA = new Qualifier("data");
        private static final Qualifier[] COLUMN_FAMILIES = new Qualifier[] {
            CF_DATA
        };

        // columns
        public static final Qualifier COL_COMMITTED = new Qualifier("committed");
        public static final Qualifier COL_ABORT = new Qualifier("abort");
        public static final Qualifier COL_MESSAGE = new Qualifier("message");

        // initial content
        private static final List<KeyValue[]> ROWS = new LinkedList<KeyValue[]>();
        static {
            // initial revision
            byte[] rowKey = Bytes.toBytes(0L);
            KeyValue[] row = {
                    new KeyValue(rowKey, CF_DATA.toBytes(),
                            COL_COMMITTED.toBytes(), Bytes.toBytes(true)),
                    new KeyValue(rowKey, CF_DATA.toBytes(),
                            COL_ABORT.toBytes(), Bytes.toBytes(false)),
                    new KeyValue(rowKey, CF_DATA.toBytes(),
                            COL_MESSAGE.toBytes(),
                            Bytes.toBytes("Initial revision"))
            };
            ROWS.add(row);
        };

        public JournalTable() {
            super(TABLE_NAME, COLUMN_FAMILIES, ROWS, 1);
        }

    }

    public static final NodeTable NODES = new NodeTable();
    public static final JournalTable JOURNAL = new JournalTable();

    public static final HBaseTableDefinition[] TABLES = {
            NODES, JOURNAL
    };

}
