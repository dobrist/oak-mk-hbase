package ch.x42.terye.mk.hbase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import ch.x42.terye.mk.hbase.HBaseTableDefinition.Qualifier;

/**
 * HBase utility methods for creating and dropping tables.
 */
public class HBaseUtils {

    public static HTable getOrCreateTable(HBaseAdmin admin,
            HBaseTableDefinition table) throws Exception {
        HTable hTable;
        try {
            hTable = new HTable(admin.getConfiguration(), table.getQualifier()
                    .toBytes());
        } catch (TableNotFoundException e) {
            // create table
            HTableDescriptor tableDesc = new HTableDescriptor(table
                    .getQualifier().toBytes());
            for (Qualifier columnFamily : table.getColumnFamilies()) {
                HColumnDescriptor colDesc = new HColumnDescriptor(
                        columnFamily.toBytes());
                colDesc.setMaxVersions(table.getMaxVersions());
                tableDesc.addFamily(colDesc);
            }
            admin.createTable(tableDesc);
            hTable = new HTable(admin.getConfiguration(), table.getQualifier()
                    .toBytes());
            // add initial content
            List<Put> batch = new LinkedList<Put>();
            for (KeyValue[] row : table.getRows()) {
                Put put = new Put(row[0].getRow());
                for (KeyValue keyValue : row) {
                    put.add(keyValue);
                }
                batch.add(put);
            }
            hTable.batch(batch);
        }
        return hTable;
    }

    public static void dropAllTables(HBaseAdmin admin) throws IOException {
        admin.disableTables(".*");
        admin.deleteTables(".*");
    }

}
