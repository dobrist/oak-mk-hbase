package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

public class GetChildNodeCountTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testNonExistingRevision() throws Exception {
        mk.getChildNodeCount("/", "10");
    }

    @Test(expected = MicroKernelException.class)
    public void testInvalidRevision() throws Exception {
        mk.getChildNodeCount("/", "abcd1234");
    }

    @Test(expected = MicroKernelException.class)
    public void testNonExistingPath() throws Exception {
        mk.getChildNodeCount("/nonexisting", null);
    }

    @Test
    public void testInitialRevision() {
        // initially the root node is the only node
        assertEquals(0, mk.getChildNodeCount("/", "0"));
        assertEquals(0, mk.getChildNodeCount("/", null));
    }

    @Test
    public void testAfterAdd() throws Exception {
        // add one node
        String r = mk.commit("/", "+\"node\":{}", null, "msg");
        // verify
        assertEquals(0, mk.getChildNodeCount("/", "0"));
        assertEquals(1, mk.getChildNodeCount("/", r));
        assertEquals(1, mk.getChildNodeCount("/", null));
    }

    @Test
    public void testAfterRemove() throws Exception {
        // add one node
        String r1 = mk.commit("/", "+\"node\":{}", null, "msg");
        // remove it
        String r2 = mk.commit("/", "-\"node\"", null, "msg");
        // verify
        assertEquals(0, mk.getChildNodeCount("/", "0"));
        assertEquals(1, mk.getChildNodeCount("/", r1));
        assertEquals(0, mk.getChildNodeCount("/", r2));
        assertEquals(0, mk.getChildNodeCount("/", null));
    }

    @Test
    public void testAfterMultipleCommits() throws Exception {
        // create a scenario
        String jsop = "+\"some\":{} +\"other\":{}";
        String r1 = mk.commit("/", jsop, null, "msg");
        jsop = "+\"node\":{ \"child\":{}, \"x\":{}}";
        String r2 = mk.commit("/", jsop, null, "msg");
        jsop = "-\"other\" +\"some/other\":{}";
        String r3 = mk.commit("/", jsop, null, "msg");
        // verify
        assertEquals(0, mk.getChildNodeCount("/", "0"));
        assertEquals(2, mk.getChildNodeCount("/", r1));
        assertEquals(0, mk.getChildNodeCount("/some", r1));
        assertEquals(0, mk.getChildNodeCount("/other", r1));
        assertEquals(3, mk.getChildNodeCount("/", r2));
        assertEquals(0, mk.getChildNodeCount("/some", r2));
        assertEquals(0, mk.getChildNodeCount("/other", r2));
        assertEquals(2, mk.getChildNodeCount("/node", r2));
        assertEquals(0, mk.getChildNodeCount("/node/child", r2));
        assertEquals(0, mk.getChildNodeCount("/node/x", r2));
        assertEquals(2, mk.getChildNodeCount("/", r3));
        assertEquals(1, mk.getChildNodeCount("/some", r3));
        assertEquals(0, mk.getChildNodeCount("/some/other", r3));
        assertEquals(2, mk.getChildNodeCount("/node", r3));
        assertEquals(0, mk.getChildNodeCount("/node/child", r3));
        assertEquals(0, mk.getChildNodeCount("/node/x", r3));
        assertEquals(2, mk.getChildNodeCount("/", null));
        assertEquals(1, mk.getChildNodeCount("/some", null));
        assertEquals(0, mk.getChildNodeCount("/some/other", null));
        assertEquals(2, mk.getChildNodeCount("/node", null));
        assertEquals(0, mk.getChildNodeCount("/node/child", null));
        assertEquals(0, mk.getChildNodeCount("/node/x", null));
    }

}
