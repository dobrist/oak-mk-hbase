package ch.x42.terye.mk.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Journal {

    private static final int TIMEOUT = 1500;
    private static final int GRACE_PERIOD = 5000;

    private HTable table;
    private AtomicLong headRevisionId;
    private LinkedList<Long> revisionIds;
    private long lastHeadRevisionId;
    private LinkedList<Long> currentRevisionIds;

    private Thread thread;
    private boolean done = false;
    private Object lock = new Object();

    public Journal(HTable table) throws IOException {
        this.table = table;
        this.headRevisionId = new AtomicLong(0);
        this.revisionIds = new LinkedList<Long>();
        this.revisionIds.add(0L);
        this.lastHeadRevisionId = 0L;
        this.currentRevisionIds = new LinkedList<Long>();
        this.currentRevisionIds.add(0L);
        // start update thread
        Updater updater = new Updater();
        updater.getNewestRevisionIds();
        thread = new Thread(updater);
        thread.setDaemon(true);
        thread.start();
    }

    public long getHeadRevisionId() {
        return headRevisionId.get();
    }

    public LinkedList<Long> getRevisionIds() {
        if (headRevisionId.get() != lastHeadRevisionId) {
            synchronized (revisionIds) {
                currentRevisionIds = new LinkedList<Long>(revisionIds);
            }
            lastHeadRevisionId = headRevisionId.get();
        }
        return currentRevisionIds;
    }

    public void addRevisionId(long revisionId) {
        synchronized (revisionIds) {
            revisionIds.add(revisionId);
            headRevisionId.set(revisionId);
        }
    }

    public void dispose() throws IOException {
        done = true;
        synchronized (lock) {
            lock.notify();
        }
        try {
            // wait for thread to die
            thread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        table.close();
    }

    private class Updater implements Runnable {

        private void getNewestRevisionIds() throws IOException {
            Scan scan = new Scan();
            long headRevId = headRevisionId.get();
            long tmp = GRACE_PERIOD << 32;
            long startRow = headRevId - tmp < 0 ? headRevId : headRevId - tmp;
            scan.setStartRow(Bytes.toBytes(startRow));
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                long id = Bytes.toLong(result.getRow());
                synchronized (revisionIds) {
                    revisionIds.add(id);
                    if (!iterator.hasNext()) {
                        headRevisionId.set(id);
                    }
                }
            }
            scanner.close();
        }

        @Override
        public void run() {
            while (!done) {
                try {
                    getNewestRevisionIds();
                    synchronized (lock) {
                        lock.wait(TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    // thread has been interrupted
                } catch (TableNotFoundException e) {
                    // might happen if journal table is being dropped
                    return;
                } catch (DoNotRetryIOException e) {
                    // might happen if journal table is being dropped
                    return;
                } catch (IOException e) {
                    // XXX: log exception
                }
            }
        }
    }

}
