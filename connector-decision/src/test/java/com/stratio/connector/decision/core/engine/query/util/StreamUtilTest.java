package com.stratio.connector.decision.core.engine.query.util;

import org.junit.Assert;
import org.junit.Test;

public class StreamUtilTest {

    @Test
    public void getDecisionAddressFormatTest() {

        String decisionAddressFormat = StreamUtil.getDecisionAddressFormat(new String[] { "10.0.0.25" },
                new String[] { "7500" });
        Assert.assertEquals("The format is not the expected", "10.0.0.25:7500", decisionAddressFormat);

        String[] hosts = new String[] { "10.0.0.25", "10.200.0.35" };
        String[] ports = new String[] { "7500", "25000" };

        decisionAddressFormat = StreamUtil.getDecisionAddressFormat(hosts, ports);
        Assert.assertEquals("The format is not the expected", "10.0.0.25:7500,10.200.0.35:25000",
                        decisionAddressFormat);

    }
}
