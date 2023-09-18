package org.apache.opendal.services;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.opendal.BaseOperatorTest;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.condition.OpenDALExceptionCondition;
import org.junit.jupiter.api.Test;

public class RedisTest extends BaseOperatorTest {

    @Override
    public String schema() {
        return "redis";
    }

    @Test
    @Override
    public void testAppend() {
        assertThatThrownBy(() -> super.testAppend())
                .is(OpenDALExceptionCondition.ofAsync(OpenDALException.Code.Unsupported));
    }

}
