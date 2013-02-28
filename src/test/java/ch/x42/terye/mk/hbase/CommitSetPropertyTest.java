package ch.x42.terye.mk.hbase;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.junit.Test;

/**
 * Tests for setting and removing properties. The tests assume the correctness
 * of:
 * <ul>
 * <li>commit(...) (adding nodes for test setup)</li>
 * <li>getNodes(...) (for test assertion)</li>
 * </ul>
 */
public class CommitSetPropertyTest extends HBaseMicroKernelTest {

    @Test(expected = MicroKernelException.class)
    public void testSetSinglePropertyNonExistingParent() throws Exception {
        // set a property of a non-existing node
        String jsop = "^\"nonexisting/myProp\":10";
        mk.commit("/", jsop, null, "test commit");
    }

    @Test(expected = MicroKernelException.class)
    public void testSetSinglePropertyNonExistingPath() throws Exception {
        // set property at a non-existing path
        String jsop = "^\"myProp\":10";
        mk.commit("/nonexisting", jsop, null, "test commit");
    }

    @Test
    public void testSetSingleProperty() throws Exception {
        // set a property
        String jsop = "^\"myInt\":10";
        String r = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myInt\":10";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
    }

    @Test
    public void testSetSinglePropertySubpath() throws Exception {
        // add a node
        String jsop = "+\"node\":{}";
        String r1 = mk.commit("/", jsop, null, "test commit");
        // set a property
        jsop = "^\"myInt\":10";
        String r2 = mk.commit("/node", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/node", r1, 9999, 0, -1, null));
        s = "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myInt\":10";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/node", r2, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/node", null, 9999, 0, -1, null));
    }

    @Test
    public void testOverwrite() throws Exception {
        // set a property using the scenario
        scenario.setProperty("/myInt", 10L);
        String r1 = scenario.commit();
        // wait for the microkernel to see the commits
        Thread.sleep(WAIT_TIMEOUT);
        // overwrite it
        String jsop = "^\"myInt\":20";
        String r2 = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myInt\":10";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r1, 9999, 0, -1, null));
        s = "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myInt\":20";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r2, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
    }

    @Test
    public void testSetMultiplePropertiesOneCommit() throws Exception {
        // set properties
        String jsop = "^\"myLong\":9223372036854775807 ";
        jsop += "^\"myString\":\"abcd efgh ijkl mnop qrst uvwx yz\" ";
        jsop += " ^\"myBoolean\":true";
        String r = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myLong\":9223372036854775807,";
        s += "  \"myString\":\"abcd efgh ijkl mnop qrst uvwx yz\",";
        s += "  \"myBoolean\":true";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
    }

    @Test
    public void testSetMultiplePropertiesOneCommitNested() throws Exception {
        // add a node
        mk.commit("/", "+\"node\":{}", null, "test commit");
        // set properties
        String jsop = "^\"node/myLong\":9223372036854775807 ";
        jsop += "+\"other\":{ \"myString\":\"abcd\", \"myBoolean\":false}";
        String r = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":2,";
        s += "  \"node\":{";
        s += "    \":childNodeCount\":0,";
        s += "    \"myLong\":9223372036854775807";
        s += "  },";
        s += "  \"other\":{";
        s += "    \":childNodeCount\":0,";
        s += "    \"myString\":\"abcd\",";
        s += "    \"myBoolean\":false";
        s += "  }";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
    }

    @Test
    public void testSetMultiplePropertiesMultipleCommits() throws Exception {
        // set properties
        String jsop = "^\"myLong\":-9223372036854775808";
        String r1 = mk.commit("/", jsop, null, "test commit");
        jsop = " ^\"myBoolean\":false";
        String r2 = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myLong\":-9223372036854775808";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r1, 9999, 0, -1, null));
        s = "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myLong\":-9223372036854775808,";
        s += "  \"myBoolean\":false";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r2, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
    }

    @Test
    public void testRemoveProperty() throws Exception {
        // set property using scenario
        scenario.setProperty("/myProperty", 1234567890L);
        String r1 = scenario.commit();
        // wait for the microkernel to see the commits
        Thread.sleep(WAIT_TIMEOUT);
        // remove it using the microkernel
        String jsop = "^\"myProperty\":null ";
        String r2 = mk.commit("/", jsop, null, "test commit");
        // verify
        String s = "";
        s += "{";
        s += "  \":childNodeCount\":0,";
        s += "  \"myProperty\":1234567890";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r1, 9999, 0, -1, null));
        s = "";
        s += "{";
        s += "  \":childNodeCount\":0";
        s += "}";
        assertJSONEquals(s, mk.getNodes("/", r2, 9999, 0, -1, null));
        assertJSONEquals(s, mk.getNodes("/", null, 9999, 0, -1, null));
    }

}
