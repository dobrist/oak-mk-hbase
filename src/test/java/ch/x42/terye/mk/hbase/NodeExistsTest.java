package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

public class NodeExistsTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testNonExistingRevision() throws Exception {
        microKernel.nodeExists("/", "10");
    }

    @Test(expected = MicroKernelException.class)
    public void testInvalidRevision() throws Exception {
        microKernel.nodeExists("/", "abcd1234");
    }

    @Test
    public void testInitialRevision() {
        // initially the root node is the only node
        assertTrue(microKernel.nodeExists("/", null));
        assertTrue(microKernel.nodeExists("/", "0"));
        assertFalse(microKernel.nodeExists("/nonexisting", null));
        assertFalse(microKernel.nodeExists("/nonexisting", "0"));
    }

    @Test
    public void testAfterAdd() throws Exception {
        // add one node
        scenario.addNode("/node");
        String r = scenario.commit();
        // wait for the microkernel to see the commit
        Thread.sleep(WAIT_TIMEOUT);
        // verify
        assertTrue(microKernel.nodeExists("/node", r));
        assertTrue(microKernel.nodeExists("/node", null));
    }

    @Test
    public void testAfterRemove() throws Exception {
        // add one node
        scenario.addNode("/node");
        String r1 = scenario.commit();
        // remove it
        scenario.removeNode("/node");
        String r2 = scenario.commit();
        // wait for the microkernel to see the commits
        Thread.sleep(WAIT_TIMEOUT);
        // verify
        assertTrue(microKernel.nodeExists("/node", r1));
        assertFalse(microKernel.nodeExists("/node", r2));
        assertFalse(microKernel.nodeExists("/node", null));
    }

    @Test
    public void testAfterReAdd() throws Exception {
        // add one node
        scenario.addNode("/node");
        String r1 = scenario.commit();
        // remove it
        scenario.removeNode("/node");
        String r2 = scenario.commit();
        // re-add it
        scenario.addNode("/node");
        String r3 = scenario.commit();
        // wait for the microkernel to see the commits
        Thread.sleep(WAIT_TIMEOUT);
        // verify
        assertTrue(microKernel.nodeExists("/node", r1));
        assertFalse(microKernel.nodeExists("/node", r2));
        assertTrue(microKernel.nodeExists("/node", r3));
        assertTrue(microKernel.nodeExists("/node", null));
    }

}
