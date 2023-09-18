package org.apache.opendal.services;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.apache.opendal.BaseOperatorTest;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.apache.opendal.utils.Utils;
import org.junit.jupiter.api.Test;

public class S3Test extends BaseOperatorTest {

    @Override
    public void initOp() {
        Optional<Operator> optional = Utils.init("s3");
        assertTrue(optional.isPresent());
        op = optional.get();
    }

    @Test
    @Override
    public void testAppend() {
        assertThatThrownBy(() -> super.testAppend())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported));
    }
}
