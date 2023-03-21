package org.apache.opendal.operator;


import org.apache.opendal.FileSystemOperator;
import org.apache.opendal.Operator;
import org.junit.Assert;
import org.junit.Test;

public class FileSystemOperatorTest {

    @Test
    public void testBuilder() {

        Operator op = new FileSystemOperator("/tmp");

        op.write("hello.txt", "hello world");
        String rs = op.read("hello.txt");
        op.delete("hello.txt");

        Assert.assertEquals(rs, "hello world");

    }

}