package org.apache.opendal;

public interface Operator {


    public void write(String s, String helloWorld);

    public String read(String s);

    public void delete(String s);
}
