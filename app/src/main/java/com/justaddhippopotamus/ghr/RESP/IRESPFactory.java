package com.justaddhippopotamus.ghr.RESP;

import java.io.IOException;
import java.io.InputStream;

public class IRESPFactory {
    public int getNextRESPInteger(InputStream in) throws IOException {
        IRESP i = getNext(in);
        if( i instanceof RESPInteger ) {
            return (int)((RESPInteger)i).value;
        }
        throw new RuntimeException("Next is not an integer.");
    }
    public IRESP getNext(InputStream in) throws IOException {
        int token = in.read();
        if( token == -1 )
            throwIOException("Ran out of bytes trying to read next token.");
        switch (token) {
            case RESPSimpleString.prefix:
                return new RESPSimpleString(in);
            case RESPSimpleError.prefix:
                return new RESPSimpleError(in);
            case RESPInteger.prefix:
                return new RESPInteger(in);
            case RESPBulkString.prefix:
                return new RESPBulkString(in);
            case RESPArray.prefix:
                return new RESPArray(this,in);
            case RESPNull.prefix:
                return new RESPNull(in);
            case RESPBoolean.prefix:
                return new RESPBoolean(in);
            case RESPDouble.prefix:
                return new RESPDouble(in);
            case RESPBigNumber.prefix:
                return new RESPBigNumber(in);
            case RESPBulkError.prefix:
                return new RESPBulkError(in);
            case RESPVerbatimString.prefix:
                return new RESPVerbatimString(in);
            case RESPMap.prefix:
                return new RESPMap(in);
            case RESPSet.prefix:
                return new RESPSet(in);
            case RESPPush.prefix:
                return new RESPPush(this,in);
            default:
                throw new IOException("Invalid initial token");
        }
    }

    public static IRESPFactory getDefault() {
        return defaultFactory;
    }

    private static final IRESPFactory defaultFactory = new IRESPFactory();

    private void throwIOException( String reason ) throws java.io.IOException {
        throw new IOException(this.getClass().getSimpleName() + ": " + reason);
    }
}
