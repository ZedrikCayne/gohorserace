package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPSimpleString extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        if( out == null) return true;
        out.write(prefix);
        writeTerminatedString(value,out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        value = readTerminatedString(in);
        return false;
    }

    public RESPSimpleString(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    @Override
    public String toString() {
        return value==null?"null":value;
    }

    public RESPSimpleString(String in) {
        value = in.replace("\n","\\n").replace("\r","\\r");
    }

    public String value = null;

    public static final char prefix = '+';
}
