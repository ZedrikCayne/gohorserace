package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPBoolean extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        if (value) out.write(value?'t':'f');
        writeCRLF(out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int readByte = in.read();
        if( readByte >= 0 ) {
            if( (char)readByte == 't') {
                value = true;
            } else if( (char)readByte == 'f') {
                value = false;
            } else {
                throwIOException("Boolean needs to be ascii value of t or f. Was " + readByte );
            }
            readExpectedCRLF(in);
        } else {
            throwIOException("Ran out of bytes.");
        }
        return false;
    }

    public boolean value = false;

    @Override
    public String toString() { return value?"TRUE":"FALSE"; }

    public RESPBoolean(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    public static final char prefix = '#';
}
