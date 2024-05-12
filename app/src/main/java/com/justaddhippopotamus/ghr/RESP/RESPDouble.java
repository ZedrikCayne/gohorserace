package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.Utils;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPDouble extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeTerminatedString(Utils.doubleToStringRedisStyle(value), out);
        return false;
    }
    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        value = Utils.stringToDoubleRedisStyle(readTerminatedString(in));
        return false;
    }
    public double value;
    public RESPDouble(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    @Override
    public String toString() { return String.valueOf(value); }
    public RESPDouble(double value) { this.value = value; }
    public static final char prefix = ',';
}
