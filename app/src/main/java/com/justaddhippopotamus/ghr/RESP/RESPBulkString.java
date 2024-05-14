package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.Server;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.types.RedisString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Base64;

public class RESPBulkString extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        if( value == null ) {
            writeTerminatedInteger(-1,out);
        } else {
            writeTerminatedInteger(value.length, out);
            out.write(value);
            writeCRLF(out);
        }
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int byteSize = readTerminatedInteger(in);
        //Special case of a 'null' BulkString where byteSize is -1
        if( byteSize >= 0 ) {
            value = in.readNBytes(byteSize);
            if( value.length != byteSize ) throwIOException("Ran out of bytes trying to read " + byteSize);
            readExpectedCRLF(in);
        } else {
            if( byteSize != -1 ) throwIOException("Invalid byte size.");
        }
        return false;
    }

    public RESPBulkString(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    public RESPBulkString(byte [] in) {
        value = in;
    }

    public RESPBulkString(byte [] from, int offset, int count) {
        value = new byte[ count ];
        System.arraycopy(from,offset,value,0,count);
    }

    public RESPBulkString(String in) {
        setValueFromString(in);
    }

    public RESPBulkString(double in) { setValueFromString(Utils.doubleToStringRedisStyle(in)); }

    private void setValueFromString(String in) {
        value = in.getBytes(Server.CHARSET);
    }

    public RESPBulkString(RedisString in) {
        if (in.isNumeric()) {
            setValueFromString(in.toString());
        } else {
            value = in.getBytes();
        }
    }

    public static RESPBulkString readFull(InputStream is) throws IOException {
        if( is.read() != prefix ) {
            throw new IOException("Not a RESPBulkString");
        }
        return new RESPBulkString(is);
    }

    public boolean isNumeric() {
        for( int i = 0; i < value.length; ++i ) {
            byte v = value[i];
            if( v < '0' || v > '9' ) {
                return false;
            }
        }
        return true;
    }

    public boolean isNumberIsh() {
        for( int i = 0; i < value.length; ++i ) {
            byte v = value [i];
            if( v < '0' || v > '9') {
                if( v == '.' ) continue;
                if( v == 'e' ) continue;
                if( v == 'E' ) continue;
                if( v == '-' ) continue;
                if( v == '+' ) continue;
                return false;
            }
        }
        return true;
    }

    @Override
    public String prettyString() {
        if( value != null ) {
            if (value.length > 96) {
                try {
                    MessageDigest md = MessageDigest.getInstance("SHA1");
                    String sha1 = Base64.getEncoder().encodeToString(md.digest(value));
                    return "RESPBulkString:" + sha1;
                } catch (Exception e) {

                }
            }
        } else {
            return "RESPBulkString: *NULL*";
        }
        return toString();
    }
    @Override
    public String toString() {
        try {
            if( value == null ) return super.toString();
            return new String(value, Server.CHARSET);
        } catch (Exception e) {
            return super.toString();
        }
    }
    public byte [] value = null;

    public static final char prefix = '$';
}
