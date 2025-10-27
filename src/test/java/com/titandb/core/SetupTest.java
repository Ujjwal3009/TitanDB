package com.titandb.core;



import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies Maven dependencies and IntelliJ configuration are correct.
 */
public class SetupTest {
    private static final Logger logger = LoggerFactory.getLogger(SetupTest.class);

    @Test
    public void testSetup() {
        logger.info("TitanDB setup test started");
        logger.debug("Guava is available: {}", com.google.common.base.Preconditions.class.getName());
        assertTrue(true, "Setup is working!");
        logger.info("🎉 Setup test passed!");
    }
}

