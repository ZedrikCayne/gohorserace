package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class RESPArray extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        out.write(prefix);
        if( value == null ) {
            writeTerminatedInteger(-1, out);
        } else {
            writeTerminatedInteger(value.size(), out);
            for( IRESP resp : value ) {
                resp.publishTo(out);
            }
        }
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int numberOfElements = readTerminatedInteger(in);
        if( numberOfElements >= 0 ) {
            value = new ArrayList<>(numberOfElements);
            for( int i = 0; i < numberOfElements; ++i ) {
                value.add(factory.getNext(in));
            }
        }
        else {
            if( numberOfElements != -1 ) {
                throwIOException("Invalid number of elements of an array. ");
            }
            readExpectedCRLF(in);
        }
        return false;
    }

    public RESPArray() {
        value = new ArrayList<>();
    }

    public RESPArray(Map<String,IRESP> map) {
        value = new ArrayList<>(map.size() * 2);
        map.forEach((k,v) -> {
            value.add(new RESPBulkString(k));
            value.add(v);
        });
    }

    public RESPArray(RedisSortedSet in) {
        in.atomic( (RedisSortedSet s) -> {
            Set<RedisSortedSet.SetValue> sortedValues = s.getSortedValues();
            int len = sortedValues.size();
            value = new ArrayList<>(len*2);
            for( RedisSortedSet.SetValue sv : sortedValues ) {
                value.add(new RESPBulkString(sv.key));
                value.add(new RESPBulkString(sv.score));
            } } );
    }

    public static RESPArray fromListOfSortedSetValues(List<RedisSortedSet.SetValue> list) {
        int len = list.size();
        RESPArray value = new RESPArray(len*2);
        for( RedisSortedSet.SetValue sv : list ) {
            value.addRespElement(new RESPBulkString(sv.key));
            value.addRespElement(new RESPBulkString(sv.score));
        }
        return value;
    }

    public static RESPArray arrayOfArraysOfSortedSetValues(List<RedisSortedSet.SetValue> list) {
        int len = list.size();
        RESPArray value = new RESPArray(len);
        for( var sv : list ) {
            RESPArray inner = new RESPArray(2);
            inner.addString(sv.key);
            inner.addString(Utils.doubleToStringRedisStyle(sv.score));
            value.addRespElement(inner);
        }
        return value;
    }

    public RESPArray(Collection<RedisType> source, IRESP.RESPVersion version) {
        value = source.stream().map( rt -> Client.getWireType(rt,version) ).collect(Collectors.toList());
    }

    public RESPArray(Collection<String> collection) {
        value = new ArrayList<>(collection.size());
        collection.forEach(c -> value.add(new RESPBulkString(c)));
    }

    public RESPArray(String...what) {
        value = Arrays.stream(what).map(RESPBulkString::new).collect(Collectors.toList());
    }

    public static RESPArray RESPArrayWithNulls(Collection<String> collection, RESPVersion v) {
        List<IRESP> value = new ArrayList<>(collection.size());
        collection.forEach(c -> {
            if( c == null ) {
                if( v == RESPVersion.RESP3 )
                    value.add(Client.NULL);
                else
                    value.add(Client.NIL_BULK_STRING);
            } else {
                value.add(new RESPBulkString(c));
            }
        });
        return new RESPArray(value);
    }


    public static RESPArray RESPArrayIntegers(Collection<Integer> collection) {
        RESPArray returnValue = new RESPArray(collection.size());
        collection.forEach( c -> returnValue.addRespElement(new RESPInteger(c)));
        return returnValue;
    }

    public static RESPArray RESPArrayDoubles(Collection<Double> collection) {
        RESPArray returnValue = new RESPArray(collection.size());
        collection.forEach( c -> returnValue.addRespElement(new RESPDouble(c)));
        return returnValue;
    }

    public static RESPArray RESPArrayRedisStrings(Collection<RedisString> collection,RESPVersion v) {
        RESPArray returnValue = new RESPArray(collection.size());
        collection.forEach( c -> {
            if( c == null ) {
                returnValue.addRespElement(v.nullFor());
            } else {
                returnValue.addRespElement(new RESPBulkString(c));
            }
        });
        return returnValue;
    }

    public RESPArray(int length) {
        value = new ArrayList<>(length);
    }

    public List<String> toStringList() {
        return value.stream().map(Object::toString).collect(Collectors.toList());
    }

    public RESPArray(List<IRESP> v) {
        value = v;
    }

    public RESPArray(RedisSet source) {
        final Set<String> theSet = source.getSet();
        value = new ArrayList<>(theSet.size());
        for( String each : theSet ) {
            value.add(new RESPBulkString(each));
        }
    }

    public void addRespElement(IRESP next) {
        value.add(next);
    }
    public void addString(String s) { value.add( new RESPBulkString(s)); }
    public void addRedisString(RedisString s) { value.add( new RESPBulkString(s)); }
    public void addInteger(int i) { value.add( new RESPInteger(i)); }

    public RESPArray(IRESPFactory factory, InputStream in) throws java.io.IOException {
        readFrom(in, factory);
    }
    public static final char prefix = '*';

    public String argAtMaybe(int index) {
        if( index >= value.size() )
            return "";
        RESPBulkString rbs = ((RESPBulkString)value.get(index));
        if( rbs == null ) {
            return "";
        }
        return rbs.toString().toUpperCase(Locale.US);
    }

    public boolean argAtMaybeIs(int index, String what ) {
        if( index >= value.size() )
            return false;
        RESPBulkString rbs = ((RESPBulkString)value.get(index));
        if( rbs == null )
            return false;
        return rbs.toString().compareToIgnoreCase(what) == 0;
    }

    public RESPBulkString rbsAt(int index) {
        if( index >= value.size() ) {
            throwError();
        }
        RESPBulkString rbs = ((RESPBulkString)value.get(index));
        if( rbs == null ) {
            throwError();
        }
        return rbs;
    }

    public double doubleAt(int index) { return Utils.stringToDoubleRedisStyle(stringAt(index) ); }
    public String stringAt(int index) {
        return rbsAt(index).toString();
    }

    public String argAt(int index) {
        return stringAt(index).toUpperCase(Locale.US);
    }

    public boolean argAtIs(int index, String what) {
        return stringAt(index).compareToIgnoreCase(what) == 0;
    }

    public RedisString redisStringAt(int index) {
        return new RedisString(rbsAt(index));
    }

    private void throwError() { throw new RuntimeException(argAt(0) + ": Bad Arguments"); }

    public int intAt(int index) {
        RESPBulkString rbs = rbsAt(index);
        try {
            return Integer.parseInt(rbs.toString());
        } catch (NumberFormatException e) {
            throwError();
        }
        return 0;
    }
    public long longAt(int index) {
        RESPBulkString rbs = rbsAt(index);
        try {
            return Long.parseLong(rbs.toString());
        } catch (NumberFormatException e) {
            throwError();
        }
        return 0;
    }

    public static RESPArray readFull(InputStream in) throws IOException {
        if( in.read() != prefix )
            throw new IOException("Not a RESPArray");
        return new RESPArray(IRESPFactory.getDefault(), in);
    }

    public RESPArray(RedisHash rh) {
        value = new ArrayList<>(rh.value.size()*2);
        for( String s : rh.value.keySet() ) {
            addRespElement(new RESPBulkString(s));
            addRespElement(rh.value.get(s).wireType(RESPVersion.RESP3));
        }
    }

    public int size() {
        if( value == null ) {
            return 0;
        }
        return value.size();
    }

    public List<String> elementsFromIndex(int index) {
        return elementsFromIndexLimit(index,0);
    }

    public List<String> elementsFromIndexLimit(int index, int limit) {
        int len = value.size();
        if( index >= len ) throwError();
        if( limit != 0 ) {
            if( limit < 0 ) {
                limit = (len - index) + limit;
                if( limit < 0 ) throwError();
            }
            if (len <= index + limit) {
                throwError();
            }
            len = Math.min(len, index + limit);
        }
        ArrayList<String> a = new ArrayList<>(len - index);
        for( int i = index; i < len; ++i ) {
            a.add(stringAt(i));
        }
        return a;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append( "RESPArray[" );
        if( value != null ) {
            for (IRESP p : value) {
                sb.append(p.prettyString());
                sb.append(",");
            }
            if (value.size() > 0)
                sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }

    public List<IRESP> value = null;

}
