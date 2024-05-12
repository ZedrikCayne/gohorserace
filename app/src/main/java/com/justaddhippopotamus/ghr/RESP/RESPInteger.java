package com.justaddhippopotamus.ghr.RESP;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RESPInteger extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeTerminatedInteger(value, out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        value = readTerminatedInteger(in);
        return false;
    }

    public RESPInteger(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    public RESPInteger(long in) {
        value = in;
    }

    public static RESPInteger readFull(InputStream in) throws IOException {
        if( in.read() != prefix )
            throw new IOException("Not a RESPInteger");
        return new RESPInteger(in);
    }

    public String toString() {
        return String.valueOf(value);
    }

    public long value = 0;

    public static final char prefix = ':';
}
