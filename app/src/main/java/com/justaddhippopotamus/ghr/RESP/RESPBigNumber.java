package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

public class RESPBigNumber extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeTerminatedString(value.toString(10),out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        String bigIntegerAsString = readTerminatedString(in);
        try {
            value = new BigInteger(bigIntegerAsString, 10);
        } catch (Exception e) {
            throwIOException("Could not parse as big integer: " + bigIntegerAsString);
        }
        return false;
    }

    public RESPBigNumber(InputStream in) throws java.io.IOException {
        readFrom(in,null );
    }

    @Override
    public String toString() { return value==null?"null":value.toString(10); }

    public BigInteger value = null;

    public static final char prefix = '(';
}
