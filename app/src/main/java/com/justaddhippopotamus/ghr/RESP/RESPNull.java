package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPNull extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeCRLF(out);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        readExpectedCRLF(in);
        return false;
    }

    public RESPNull(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    public RESPNull() {
    }

    @Override
    public String toString() { return "RESPNull"; }

    public static final char prefix = '_';

}
