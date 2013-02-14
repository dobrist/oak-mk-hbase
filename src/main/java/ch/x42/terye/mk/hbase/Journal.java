package ch.x42.terye.mk.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jackrabbit.mk.api.MicroKernelException;

import ch.x42.terye.mk.hbase.HBaseMicroKernelSchema.JournalTable;

public class Journal {

    // read journal table every so many milliseconds
    private static final int TIMEOUT = 1500;
    // grace period for long-taking tries of revisions (a commit, where the
    // successful try took longer than the grace period might not be seen by
    // other microkernels)
    public static final int GRACE_PERIOD = 800;

    private HTable table;
    public LinkedHashSet<Long> journal;
    private long headRevisionId;

    private Thread thread;
    private boolean done = false;
    private Object timeoutLock;
    private Object updateLock;

    public Journal(HTable table) throws IOException {
        this.table = table;
        this.journal = new LinkedHashSet<Long>();
        this.journal.add(0L);
        this.timeoutLock = new Object();
        this.updateLock = new Object();

        // start update thread
        Updater updater = new Updater();
        updater.updateJournal();
        thread = new Thread(updater);
        thread.setDaemon(true);
        thread.start();
    }

    public void update() {
        try {
            synchronized (updateLock) {
                synchronized (timeoutLock) {
                    timeoutLock.notify();
                }
                updateLock.wait();
            }
        } catch (InterruptedException e) {
            // thread has been notified
        }
    }

    public long getHeadRevisionId() {
        synchronized (journal) {
            return headRevisionId;
        }
    }

    /**
     * Returns the complete journal.
     */
    public LinkedList<Long> getJournal() {
        synchronized (journal) {
            return new LinkedList<Long>(journal);
        }
    }

    /**
     * Returns the journal up to and including the specified revision id.
     * 
     * @param revisionId a revision id
     * @return the journal up to and including the specified revision id
     * @throws MicroKernelException when specified revision id is not present in
     *             journal
     */
    public LinkedList<Long> getJournal(long revisionId) {
        LinkedList<Long> revisionIds = getJournal();
        boolean found = false;
        Iterator<Long> iterator = revisionIds.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() == revisionId) {
                found = true;
            } else if (found) {
                iterator.remove();
            }
        }
        if (!found) {
            throw new MicroKernelException("Unknown revision id " + revisionId);
        }
        return revisionIds;
    }

    public void addRevisionId(long revisionId) {
        synchronized (journal) {
            journal.add(revisionId);
            headRevisionId = revisionId;
        }
    }

    public void dispose() throws IOException {
        done = true;
        synchronized (timeoutLock) {
            timeoutLock.notify();
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

        private Long lastTimeRead;

        private void updateJournal() throws IOException {
            Scan scan = new Scan();
            if (lastTimeRead != null) {
                // only scan what hasn't been scanned yet (giving potential
                // long-taking revisions a grace period of GRACE_PERIOD ms)
                long timestamp = lastTimeRead - HBaseMicroKernel.EPOCH
                        - GRACE_PERIOD;
                scan.setStartRow(Bytes.toBytes(timestamp << 24));
            }
            lastTimeRead = System.currentTimeMillis();
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()) {
                Result result = iterator.next();
                // discard uncommitted revisions
                if (!Bytes.toBoolean(result.getValue(
                        JournalTable.CF_DATA.toBytes(),
                        JournalTable.COL_COMMITTED.toBytes()))) {
                    continue;
                }
                long id = Bytes.toLong(result.getRow());
                synchronized (journal) {
                    // discard if already present
                    if (!journal.contains(id)) {
                        // add revision to journal
                        journal.add(id);
                        headRevisionId = id;
                    }
                }
            }
            scanner.close();
        }

        @Override
        public void run() {
            while (!done) {
                try {
                    updateJournal();
                    synchronized (updateLock) {
                        updateLock.notify();
                    }
                    synchronized (timeoutLock) {
                        timeoutLock.wait(TIMEOUT);
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
