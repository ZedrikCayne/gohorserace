package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.types.RedisHash;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class RESPMap extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        writeTerminatedInteger(value.size(), out);
        for(IRESP key : value.keySet() ) {
            key.publishTo(out);
            value.get(key).publishTo(out);
        }
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int size = readTerminatedInteger(in);
        value = new LinkedHashMap<>(size);
        for( int i = 0; i < size; ++i ) {
            IRESP key = factory.getNext(in);
            IRESP val = factory.getNext(in);
            value.put(key,val);
        }

        return false;
    }

    public RESPMap(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    public RESPMap(RedisSortedSet in) {

    }

    public RESPMap(Map<String,IRESP> map) {
        value = new LinkedHashMap<>(map.size());
        map.forEach((k,v) -> value.put(new RESPBulkString(k),v));
    }

    @Override
    public String toString() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("RESPMap{");
            String prefix = "";
            for (var k : value.keySet()) {
                sb.append(prefix);
                prefix = ",";
                sb.append(k.toString());
                sb.append(':');
                sb.append(value.get(k).toString());
            }
            sb.append('}');
            return sb.toString();
        } catch (Exception e) {
            return super.toString();
        }
    }

    public RESPMap(RedisHash rh) {
        value = new LinkedHashMap<>();
        rh.value.forEach((k,v) -> {
            value.put(new RESPBulkString(k), new RESPBulkString(v) );
        });
    }

    LinkedHashMap<IRESP,IRESP> value = null;
    public static final char prefix = '%';
}
