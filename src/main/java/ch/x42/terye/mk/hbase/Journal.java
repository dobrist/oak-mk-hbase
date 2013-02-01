package ch.x42.terye.mk.hbase;

import java.io.IOException;
import java.util.LinkedHashSet;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Journal {

    private static final int TIMEOUT = 1500;
    private static final int GRACE_PERIOD = 5000;

    private HTable table;
    private long headRevisionId;
    private LinkedHashSet<Long> revisionIds;

    public Journal(HTable table) {
        this.table = table;
        this.headRevisionId = 0L;
        this.revisionIds = new LinkedHashSet<Long>();
        // start update thread
        Thread thread = new Thread(new Updater());
        thread.start();
    }

    public long getHeadRevisionId() {
        return headRevisionId;
    }

    private class Updater implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    Scan scan = new Scan();
                    long tmp = GRACE_PERIOD << 32;
                    long startRow = headRevisionId - tmp < 0 ? headRevisionId
                            : headRevisionId - tmp;
                    scan.setStartRow(Bytes.toBytes(startRow));
                    ResultScanner scanner = table.getScanner(scan);
                    for (Result result : scanner) {
                        revisionIds.add(Bytes.toLong(result.getRow()));
                    }
                    Thread.sleep(TIMEOUT);
                } catch (IOException e) {
                } catch (InterruptedException e) {
                }
            }
        }
    }

}
