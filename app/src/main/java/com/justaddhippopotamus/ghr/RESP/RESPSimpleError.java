package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPSimpleError extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeTerminatedString(value, out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        value = readTerminatedString(in);
        return false;
    }

    public RESPSimpleError(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    @Override
    public String toString() {
        return value==null?"null":value;
    }

    public RESPSimpleError(String error) {
        value = error.replace("\n","\\n").replace("\r","\\r");
    }

    public static final char prefix = '-';

    public String value = null;
}
