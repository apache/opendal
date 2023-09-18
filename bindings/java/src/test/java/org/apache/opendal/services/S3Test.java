package org.apache.opendal.services;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Optional;
import org.apache.opendal.BaseOperatorTest;
import org.apache.opendal.Operator;
import org.apache.opendal.utils.Utils;

public class S3Test extends BaseOperatorTest {

    @Override
    public void initOp() {
        Optional<Operator> optional = Utils.init("S3");
        assertTrue(optional.isPresent());
        op = optional.get();
    }
}
