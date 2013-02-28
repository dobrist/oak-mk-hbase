package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

/**
 * Tests for testing node existence.
 */
public class NodeExistsTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testNonExistingRevision() throws Exception {
        mk.nodeExists("/", "10");
    }

    @Test(expected = MicroKernelException.class)
    public void testInvalidRevision() throws Exception {
        mk.nodeExists("/", "abcd1234");
    }

    @Test
    public void testInitialRevision() {
        // initially the root node is the only node
        assertTrue(mk.nodeExists("/", null));
        assertTrue(mk.nodeExists("/", "0"));
        assertFalse(mk.nodeExists("/nonexisting", null));
        assertFalse(mk.nodeExists("/nonexisting", "0"));
    }

    @Test
    public void testAfterAdd() throws Exception {
        // add one node
        scenario.addNode("/node");
        String r = scenario.commit();
        // wait for the microkernel to see the commit
        Thread.sleep(WAIT_TIMEOUT);
        // verify
        assertTrue(mk.nodeExists("/node", r));
        assertTrue(mk.nodeExists("/node", null));
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
        assertTrue(mk.nodeExists("/node", r1));
        assertFalse(mk.nodeExists("/node", r2));
        assertFalse(mk.nodeExists("/node", null));
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
        assertTrue(mk.nodeExists("/node", r1));
        assertFalse(mk.nodeExists("/node", r2));
        assertTrue(mk.nodeExists("/node", r3));
        assertTrue(mk.nodeExists("/node", null));
    }

}
