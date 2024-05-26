package com.justaddhippopotamus.ghr.server.types;

import java.io.IOException;
import java.io.InputStream;

public class RedisTypeFactory {
    public static RedisTypeFactory getDefault() {
        return defaultFactory;
    }
    private static final RedisTypeFactory defaultFactory = new RedisTypeFactory();
    public void RedisTypeFactory() {}

    public RedisType from(InputStream is) throws IOException {
        int next = is.read();
        switch( next ) {
            case RedisString.prefix:
                return new RedisString(is);
            case RedisSet.prefix:
                return new RedisSet(is);
            case RedisSortedSet.prefix:
                return new RedisSortedSet(is);
            case RedisList.prefix:
                return new RedisList(is);
            case RedisHash.prefix:
                return new RedisHash(is);
            case RedisStream.prefix:
                return new RedisStream(is);
            case RedisHyperLogLog.prefix:
                return new RedisHyperLogLog(new RedisString(is));
            case -1:
                return null;
            default:
                throw new RuntimeException("Stream has bad format");
        }
    }
}
