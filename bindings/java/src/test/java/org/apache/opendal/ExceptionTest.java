package org.apache.opendal;

import java.util.HashMap;
import java.util.Map;
import org.apache.opendal.exception.OpenDALErrorCode;
import org.apache.opendal.exception.OpenDALException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExceptionTest {
    Operator operator;

    @BeforeEach
    public void init() {
        Map<String, String> params = new HashMap<>();
        params.put("root", "/tmp");
        this.operator = new Operator("Memory", params);
    }

    @AfterEach
    public void clean() {
        this.operator.close();
    }

    @Test
    public void testStatNotExistFile() {
        OpenDALException exception = assertThrows(OpenDALException.class, () -> this.operator.stat("not_exist_file"));
        assertEquals(exception.getErrorCode(), OpenDALErrorCode.NOT_FOUND);
    }
}
