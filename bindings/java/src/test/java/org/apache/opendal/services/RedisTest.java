package org.apache.opendal.services;

import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.opendal.BaseOperatorTest;
import org.apache.opendal.Operator;
import org.apache.opendal.utils.Utils;

public class RedisTest extends BaseOperatorTest {

    @Override
    public void initOp() {
        Optional<Operator> optional = Utils.init("redis");
        assertTrue(optional.isPresent());
        op = optional.get();
    }

}
