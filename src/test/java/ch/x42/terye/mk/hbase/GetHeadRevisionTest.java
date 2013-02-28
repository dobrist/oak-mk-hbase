package ch.x42.terye.mk.hbase;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests for setting the head revision. The tests assume the correctness of:
 * <ul>
 * <li>commit(...) (for test setup)</li>
 * </ul>
 */
public class GetHeadRevisionTest extends HBaseMicroKernelTest {

    @Test
    public void testInitialRevision() {
        assertEquals("0", mk.getHeadRevision());
    }

    @Test
    public void testAfterCommit() throws Exception {
        // make one commit
        String r = mk.commit("/", "+\"node\":{}", null, "test commit");
        // verify
        assertEquals(r, mk.getHeadRevision());
    }

    @Test
    public void testAfterMultipleCommit() throws Exception {
        // make multiple commits
        mk.commit("/", "+\"node\":{}", null, "test commit");
        mk.commit("/", "+\"node/child\":{}", null, "test commit");
        String r = mk.commit("/", "+\"child\":{}", null, "test commit");
        // verify
        assertEquals(r, mk.getHeadRevision());
    }

}
