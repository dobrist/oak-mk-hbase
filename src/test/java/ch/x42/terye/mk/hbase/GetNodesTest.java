package ch.x42.terye.mk.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

public class GetNodesTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testNonExistingRevision() throws Exception {
        microKernel.getNodes("/", "10", 0, 0, -1, null);
    }

    @Test(expected = MicroKernelException.class)
    public void testInvalidRevision() throws Exception {
        microKernel.getNodes("/", "abcd1234", 0, 0, -1, null);
    }

    @Test
    public void testInitialRevision() throws Exception {
        // expected JSON result
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0";
        s += "}";
        // get nodes
        String result = microKernel.getNodes("/", "0", 9999, 0, -1, null);
        // assert JSON equality
        assertJSONEquals(s, result);
    }

    private String[] createScenario() throws Exception {
        List<String> revisionIds = new ArrayList<String>();
        scenario.addNode("/a");
        scenario.setProperty("/a/p", "abcdefgh");
        scenario.setProperty("/a/q", 922337203685477580L);
        scenario.setProperty("/a/r", true);
        revisionIds.add(scenario.commit());
        scenario.addNode("/a/b");
        scenario.addNode("/c");
        scenario.addNode("/c/d");
        scenario.addNode("/c/d/e");
        scenario.addNode("/c/f");
        scenario.setProperty("/s", "ijklmnop");
        revisionIds.add(scenario.commit());
        Thread.sleep(HBaseMicroKernel.JOURNAL_UPDATE_TIMEOUT + 500);
        return revisionIds.toArray(new String[0]);
    }

    @Test
    public void testImplicitHeadRevision() throws Exception {
        createScenario();
        // expected JSON result
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":2,";
        s += "  \"s\":\"ijklmnop\",";
        s += "  \"a\":{";
        s += "    \":childNodeCount\":1,";
        s += "    \"p\":\"abcdefgh\",";
        s += "    \"q\":922337203685477580,";
        s += "    \"r\":true,";
        s += "    \"b\":{";
        s += "      \":childNodeCount\":0";
        s += "    }";
        s += "  },";
        s += "  \"c\":{";
        s += "    \":childNodeCount\":2,";
        s += "    \"d\":{";
        s += "      \":childNodeCount\":1,";
        s += "      \"e\":{";
        s += "        \":childNodeCount\":0";
        s += "      }";
        s += "    },";
        s += "    \"f\":{";
        s += "      \":childNodeCount\":0";
        s += "    }";
        s += "  }";
        s += "}";
        // get nodes
        String result = microKernel.getNodes("/", null, 9999, 0, -1, null);
        // assert JSON equality
        assertJSONEquals(s, result);
    }

    @Test
    public void testIndividualRevisions() throws Exception {
        String[] revIds = createScenario();
        // expected JSON results
        String s1 = "";
        s1 += "{";
        s1 += "  \":childNodeCount\":1,";
        s1 += "  \"a\":{";
        s1 += "    \":childNodeCount\":0,";
        s1 += "    \"p\":\"abcdefgh\",";
        s1 += "    \"q\":922337203685477580,";
        s1 += "    \"r\":true";
        s1 += "  }";
        s1 += "}";
        String s2 = "";
        s2 += "{";
        s2 += "  \":childNodeCount\":2,";
        s2 += "  \"s\":\"ijklmnop\",";
        s2 += "  \"a\":{";
        s2 += "    \":childNodeCount\":1,";
        s2 += "    \"p\":\"abcdefgh\",";
        s2 += "    \"q\":922337203685477580,";
        s2 += "    \"r\":true,";
        s2 += "    \"b\":{";
        s2 += "      \":childNodeCount\":0";
        s2 += "    }";
        s2 += "  },";
        s2 += "  \"c\":{";
        s2 += "    \":childNodeCount\":2,";
        s2 += "    \"d\":{";
        s2 += "      \":childNodeCount\":1,";
        s2 += "      \"e\":{";
        s2 += "        \":childNodeCount\":0";
        s2 += "      }";
        s2 += "    },";
        s2 += "    \"f\":{";
        s2 += "      \":childNodeCount\":0";
        s2 += "    }";
        s2 += "  }";
        s2 += "}";
        // get nodes
        String res1 = microKernel.getNodes("/", revIds[0], 9999, 0, -1, null);
        String res2 = microKernel.getNodes("/", revIds[1], 9999, 0, -1, null);
        // assert JSON equality
        assertJSONEquals(s1, res1);
        assertJSONEquals(s2, res2);
    }

    @Test
    public void testSubpath() throws Exception {
        createScenario();
        // expected JSON result
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":1,";
        s += "  \"e\":{";
        s += "    \":childNodeCount\":0";
        s += "  }";
        s += "}";
        // get nodes
        String result = microKernel.getNodes("/c/d", null, 9999, 0, -1, null);
        // assert JSON equality
        assertJSONEquals(s, result);
    }

    @Test
    public void testLimitedDepth() throws Exception {
        createScenario();
        // expected JSON result
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":2,";
        s += "  \"s\":\"ijklmnop\",";
        s += "  \"a\":{";
        s += "    \":childNodeCount\":1,";
        s += "    \"p\":\"abcdefgh\",";
        s += "    \"q\":922337203685477580,";
        s += "    \"r\":true,";
        s += "    \"b\":{";
        s += "    }";
        s += "  },";
        s += "  \"c\":{";
        s += "    \":childNodeCount\":2,";
        s += "    \"d\":{";
        s += "    },";
        s += "    \"f\":{";
        s += "    }";
        s += "  }";
        s += "}";
        // get nodes
        String result = microKernel.getNodes("/", null, 1, 0, -1, null);
        // assert JSON equality
        assertJSONEquals(s, result);
    }

    @Test
    public void testZeroDepth() throws Exception {
        createScenario();
        // expected JSON result
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":2,";
        s += "  \"s\":\"ijklmnop\",";
        s += "  \"a\":{";
        s += "  },";
        s += "  \"c\":{";
        s += "  }";
        s += "}";
        // get nodes
        String result = microKernel.getNodes("/", null, 0, 0, -1, null);
        // assert JSON equality
        assertJSONEquals(s, result);
    }

}
