package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

/**
 * Tests for adding nodes. The tests assume the correctness of:
 * <ul>
 * <li>getNodes(...) (for test assertion)</li>
 * <li>nodeExists(...) (for test assertion)</li>
 * </ul>
 */
public class CommitAddNodeTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testAddSingleNodeNonExistingIntermediate() throws Exception {
        // add node with non-existing intermediate nodes
        String jsop = "+\"nonexisting/node\":{}";
        mk.commit("/", jsop, null, "test commit");
    }

    @Test(expected = MicroKernelException.class)
    public void testAddSingleNodeNonExistingPath() throws Exception {
        // add node at non-existing path
        String jsop = "+\"node\":{}";
        mk.commit("/nonexisting", jsop, null, "test commit");
    }

    @Test
    public void testAddSingleNode() throws Exception {
        // add one node
        String jsop = "+\"node\":{}";
        String r = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/node", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/node", null, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/node", r));
        assertTrue(mk.nodeExists("/node", null));
    }

    @Test(expected = MicroKernelException.class)
    public void testAddDuplicate() throws Exception {
        // add a node using the scenario
        scenario.addNode("/node");
        scenario.commit();
        // add already existing node
        String jsop = "+\"node\":{}";
        mk.commit("/", jsop, null, "test commit");
    }

    @Test
    public void testAddSingleNodeSubpath() throws Exception {
        // add a node using the scenario
        scenario.addNode("/node");
        scenario.commit();
        // wait for the microkernel to see the commits
        Thread.sleep(WAIT_TIMEOUT);
        // add one node
        String jsop = "+\"child\":{}";
        String r = mk.commit("/node", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":1,";
        s += "  \"child\":{";
        s += "    \":childNodeCount\":0";
        s += "  }";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/node", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/node", null, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/node/child", r));
        assertTrue(mk.nodeExists("/node/child", null));
    }

    @Test
    public void testAddMultipleNodes() throws Exception {
        // add nodes
        String jsop = "+\"child\":{} +\"other\":{} +\"third\":{}";
        String r = mk.commit("/", jsop, null, "test commit");
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
        assertJSONEquals(s, mk.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/child", null));
        assertTrue(mk.nodeExists("/other", null));
        assertTrue(mk.nodeExists("/third", null));
        assertTrue(mk.nodeExists("/child", r));
        assertTrue(mk.nodeExists("/other", r));
        assertTrue(mk.nodeExists("/third", r));
    }

    @Test
    public void testAddMultipleNestedNodes() throws Exception {
        // add nodes
        String jsop = "+\"node\":{ \"child\":{ \"third-level\":{}, \"sibling\":{}}, \"x\":{ \"y\":{ \"z\":{} } }}";
        String r = mk.commit("/", jsop, null, "test commit");
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
        assertJSONEquals(s, mk.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/node", r));
        assertTrue(mk.nodeExists("/node/child", r));
        assertTrue(mk.nodeExists("/node/child/third-level", r));
        assertTrue(mk.nodeExists("/node/child/sibling", r));
        assertTrue(mk.nodeExists("/node/x", r));
        assertTrue(mk.nodeExists("/node/x/y", r));
        assertTrue(mk.nodeExists("/node/x/y/z", r));
        assertTrue(mk.nodeExists("/node", null));
        assertTrue(mk.nodeExists("/node/child", null));
        assertTrue(mk.nodeExists("/node/child/third-level", null));
        assertTrue(mk.nodeExists("/node/child/sibling", null));
        assertTrue(mk.nodeExists("/node/x", null));
        assertTrue(mk.nodeExists("/node/x/y", null));
        assertTrue(mk.nodeExists("/node/x/y/z", null));
    }

    @Test
    public void testAddMultipleNodesMultipleCommits() throws Exception {
        // add nodes
        String jsop = "+\"child\":{}";
        String r1 = mk.commit("/", jsop, null, "test commit");
        jsop = "+\"other\":{ \"third\":{}} +\"child/child\":{}";
        String r2 = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":1,";
        s += "  \"child\":{";
        s += "    \":childNodeCount\":0";
        s += "  }";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r1, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/child", r1));
        assertFalse(mk.nodeExists("/child/child", r1));
        assertFalse(mk.nodeExists("/other", r1));
        assertFalse(mk.nodeExists("/other/third", r1));
        s = "{";
        s += "  \":childNodeCount\":2,";
        s += "  \"child\":{";
        s += "    \":childNodeCount\":1,";
        s += "    \"child\":{";
        s += "      \":childNodeCount\":0";
        s += "    }";
        s += "  },";
        s += "  \"other\":{";
        s += "    \":childNodeCount\":1,";
        s += "    \"third\":{";
        s += "      \":childNodeCount\":0";
        s += "    }";
        s += "  }";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r2, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/child", r2));
        assertTrue(mk.nodeExists("/child/child", r2));
        assertTrue(mk.nodeExists("/other", r2));
        assertTrue(mk.nodeExists("/other/third", r2));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
        assertTrue(mk.nodeExists("/child", null));
        assertTrue(mk.nodeExists("/child/child", null));
        assertTrue(mk.nodeExists("/other", null));
        assertTrue(mk.nodeExists("/other/third", null));
    }
}
