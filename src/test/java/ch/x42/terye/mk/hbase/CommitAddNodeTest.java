package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

/**
 * These tests use HBaseMicroKernel.nodeExists(...) and
 * HBaseMicroKernel.getNodes(...) for test assertion and aussem them to be
 * working and correct.
 */
public class CommitAddNodeTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testAddSingleNodeNonExistingIntermediate() throws Exception {
        // add node with non-existing intermediate nodes
        String jsop = "+\"nonexisting/node\":{}";
        microKernel.commit("/", jsop, null, "test commit");
    }

    @Test(expected = MicroKernelException.class)
    public void testAddSingleNodeNonExistingPath() throws Exception {
        // add node with non-existing intermediate nodes
        String jsop = "+\"node\":{}";
        microKernel.commit("/nonexisting", jsop, null, "test commit");
    }

    @Test
    public void testAddSingleNode() throws Exception {
        // add one node
        String jsop = "+\"node\":{}";
        String r = microKernel.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0";
        s += "}";
        assertJSONEquals(s, microKernel.getNodes("/node", r, 9999, 0, -1, null));
        assertJSONEquals(s,
                microKernel.getNodes("/node", null, 9999, 0, -1, null));
        assertTrue(microKernel.nodeExists("/node", r));
        assertTrue(microKernel.nodeExists("/node", null));
    }

    @Test(expected = MicroKernelException.class)
    public void testAddDuplicate() throws Exception {
        // add a node using the scenario
        scenario.addNode("/node");
        scenario.commit();
        // add already existing node
        String jsop = "+\"node\":{}";
        microKernel.commit("/", jsop, null, "test commit");
    }

    @Test
    public void testAddSingleNodeSubPath() throws Exception {
        // add a node using the scenario
        scenario.addNode("/node");
        scenario.commit();
        // wait for the microkernel to see the commits
        Thread.sleep(WAIT_TIMEOUT);
        // add one node
        String jsop = "+\"child\":{}";
        String r = microKernel.commit("/node", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":1,";
        s += "  \"child\":{";
        s += "    \":childNodeCount\":0";
        s += "  }";
        s += "}";
        assertJSONEquals(s, microKernel.getNodes("/node", r, 9999, 0, -1, null));
        assertJSONEquals(s,
                microKernel.getNodes("/node", null, 9999, 0, -1, null));
        assertTrue(microKernel.nodeExists("/node/child", r));
        assertTrue(microKernel.nodeExists("/node/child", null));
    }

    @Test
    public void testAddMultipleNodes() throws Exception {
        // add nodes
        String jsop = "+\"child\":{} +\"other\":{} +\"third\":{}";
        String r = microKernel.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":3,";
        s += "  \"child\":{";
        s += "    \":childNodeCount\":0";
        s += "  },";
        s += "  \"other\":{";
        s += "    \":childNodeCount\":0";
        s += "  },";
        s += "  \"third\":{";
        s += "    \":childNodeCount\":0";
        s += "  }";
        s += "}";
        assertJSONEquals(s, microKernel.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, microKernel.getNodes("/", null, 9999, 0, -1, null));
        assertTrue(microKernel.nodeExists("/child", null));
        assertTrue(microKernel.nodeExists("/other", null));
        assertTrue(microKernel.nodeExists("/third", null));
        assertTrue(microKernel.nodeExists("/child", r));
        assertTrue(microKernel.nodeExists("/other", r));
        assertTrue(microKernel.nodeExists("/third", r));
    }

    @Test
    public void testAddMultipleNestedNodes() throws Exception {
        // add nodes
        String jsop = "+\"node\":{ \"child\":{ \"third-level\":{}, \"sibling\":{}}, \"x\":{ \"y\":{ \"z\":{} } }}";
        String r = microKernel.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":1,";
        s += "  \"node\":{";
        s += "    \":childNodeCount\":2,";
        s += "    \"child\":{";
        s += "      \":childNodeCount\":2,";
        s += "      \"third-level\":{";
        s += "        \":childNodeCount\":0";
        s += "      },";
        s += "      \"sibling\":{";
        s += "        \":childNodeCount\":0";
        s += "      }";
        s += "    },";
        s += "    \"x\":{";
        s += "      \":childNodeCount\":1,";
        s += "      \"y\":{";
        s += "        \":childNodeCount\":1,";
        s += "        \"z\":{";
        s += "          \":childNodeCount\":0";
        s += "        }";
        s += "      }";
        s += "    }";
        s += "  }";
        s += "}";
        assertJSONEquals(s, microKernel.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, microKernel.getNodes("/", null, 9999, 0, -1, null));
        assertTrue(microKernel.nodeExists("/node", r));
        assertTrue(microKernel.nodeExists("/node/child", r));
        assertTrue(microKernel.nodeExists("/node/child/third-level", r));
        assertTrue(microKernel.nodeExists("/node/child/sibling", r));
        assertTrue(microKernel.nodeExists("/node/x", r));
        assertTrue(microKernel.nodeExists("/node/x/y", r));
        assertTrue(microKernel.nodeExists("/node/x/y/z", r));
        assertTrue(microKernel.nodeExists("/node", null));
        assertTrue(microKernel.nodeExists("/node/child", null));
        assertTrue(microKernel.nodeExists("/node/child/third-level", null));
        assertTrue(microKernel.nodeExists("/node/child/sibling", null));
        assertTrue(microKernel.nodeExists("/node/x", null));
        assertTrue(microKernel.nodeExists("/node/x/y", null));
        assertTrue(microKernel.nodeExists("/node/x/y/z", null));
    }

}
