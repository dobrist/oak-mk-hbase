package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class GetHeadRevisionTest extends HBaseMicroKernelTest {

    @Test
    public void testInitialRevision() {
        assertEquals("0", mk.getHeadRevision());
    }

    @Test
    public void testAfterCommit() throws Exception {
        // add one node
        scenario.addNode("/node");
        String r = scenario.commit();
        // wait for the microkernel to see the commit
        Thread.sleep(WAIT_TIMEOUT);
        // verify
        assertEquals(r, mk.getHeadRevision());
    }

}
