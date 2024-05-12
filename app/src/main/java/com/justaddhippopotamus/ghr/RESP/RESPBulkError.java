package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.Server;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPBulkError extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        writeTerminatedStringWithSize(value, out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int numberOfBytesToRead = readTerminatedInteger(in);
        byte [] bytes = in.readNBytes(numberOfBytesToRead);
        if( bytes.length != numberOfBytesToRead )
            throwIOException("Ran out of bytes reading error.");
        readExpectedCRLF(in);
        value = new String(bytes, Server.CHARSET);
        return false;
    }

    public String value = null;

    @Override
    public String toString() { return value==null?"ERROR:null":"ERROR: "+value; }

    public RESPBulkError(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }
    public static final char prefix = '!';
}
