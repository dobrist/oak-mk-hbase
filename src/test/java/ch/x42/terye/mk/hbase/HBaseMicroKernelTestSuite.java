package ch.x42.terye.mk.hbase;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Test suite that executes all HBaseMicroKernel tests in the correct order.
 */
@RunWith(Suite.class)
@SuiteClasses({
        NodeExistsTest.class, GetNodesTest.class, CommitAddNodeTest.class,
        CommitSetPropertyTest.class, GetHeadRevisionTest.class,
        GetChildNodeCountTest.class
})
public class HBaseMicroKernelTestSuite {

}
