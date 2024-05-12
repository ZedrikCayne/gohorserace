package com.justaddhippopotamus.ghr.server.types;

import com.justaddhippopotamus.ghr.RESP.IRESP;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RedisStream extends RedisType {
    public static final char prefix = '>';
    @Override
    public String type() {
        return "stream";
    }

    public RedisStream() {

    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        os.write(prefix);
    }

    @Override
    public void readFrom(InputStream is) throws IOException {

    }

    @Override
    public IRESP wireType(IRESP.RESPVersion v) {
        return null;
    }

    public RedisStream(InputStream is) throws IOException {
        readFrom(is);
    }

    @Override
    public <T extends RedisType> T copy(Class<T> type) {
        return null;
    }
}
