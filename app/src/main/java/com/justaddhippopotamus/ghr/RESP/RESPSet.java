package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

public class RESPSet extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeTerminatedInteger(value.size(), out);
        for (IRESP a : value) {
            a.publishTo(out);
        }
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int size = readTerminatedInteger(in);
        value = new HashSet<>(size);
        for( int i = 0; i < size; ++i ) {
            value.add( factory.getNext(in) );
        }
        return false;
    }

    public RESPSet(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    @Override
    public String toString() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("RESPSet{");
            String prefix = "";
            for (IRESP in : value) {
                sb.append(prefix);
                prefix = ",";
                if (in == null) sb.append("null");
                else sb.append(in.toString());
            }
            sb.append("}");
            return sb.toString();
        } catch (Exception e) {
            return super.toString();
        }
    }

    public static final char prefix = '~';

    public Set<IRESP> value;
}
