package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.Server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class IRESP {
    abstract public boolean publishTo(OutputStream out) throws java.io.IOException;
    abstract public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException;

    protected void writeTerminatedStringWithSize(String toWrite, OutputStream out ) throws java.io.IOException {
        writeTerminatedBytesWithSize(toWrite.getBytes(Server.CHARSET),out);
    }

    public String prettyString() {
        return toString();
    }

    protected void writeTerminatedBytesWithSize(byte [] toWrite, OutputStream out ) throws java.io.IOException {
        writeTerminatedInteger( toWrite.length, out );
        out.write(toWrite);
        writeCRLF(out);
    }
    protected void writeTerminatedString(String toWrite, OutputStream out) throws java.io.IOException {
        out.write(toWrite.getBytes(Server.CHARSET));
        writeCRLF(out);
    }

    protected void writeTerminatedInteger(long toWrite, OutputStream out ) throws java.io.IOException {
        writeTerminatedString(String.valueOf(toWrite),out);
    }

    protected String readTerminatedString(InputStream in) throws java.io.IOException {
        StringBuilder sb = new StringBuilder();
        int inChar = in.read();
        while(inChar > 0 && inChar != IRESP.CR)
        {
            sb.append( (char)inChar );
            inChar = in.read();
        }
        if( inChar < 0 )
            throwIOException("Ran out of data trying to read a string");
        if( in.read() != IRESP.LF )
            throwIOException("String did not end with a CR/LF pair");

        return sb.toString();
    }

    protected int readTerminatedInteger(InputStream in) throws java.io.IOException {
        String integerAsString = readTerminatedString(in);
        int returnInteger = 0;
        try {
            returnInteger = Integer.parseInt(integerAsString,10);
        } catch (Exception e) {
            throwIOException("Could not parse integer out of " + integerAsString);
        }
        return returnInteger;
    }

    protected void readExpectedCRLF(InputStream in) throws java.io.IOException {
        if( in.read() != CR || in.read() != LF )
            throwIOException("Failed to read expected CRLF");
    }

    protected void writeCRLF(OutputStream out) throws java.io.IOException {
        out.write(CR);
        out.write(LF);
    }

    protected void throwIOException(String reason) throws java.io.IOException {
        throw new IOException(this.getClass().getSimpleName() + ": " + reason);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
    public static final int CR = 13;
    public static final int LF = 10;

    public static final int SPACE = 32;

    public enum RESPVersion {
        RESP2,
        RESP3
    }
}
