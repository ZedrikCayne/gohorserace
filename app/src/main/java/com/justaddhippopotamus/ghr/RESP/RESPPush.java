package com.justaddhippopotamus.ghr.RESP;

import java.io.InputStream;
import java.io.OutputStream;

//Pushes are weird....they look like arrays in resp2 and pushes in resp3. So let's just
//tread them like it.
public class RESPPush extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        if( resp3 ) out.write(prefix);
        else out.write(resp2Prefix);
        writeTerminatedInteger(value.length,out);
        for( var val : value ) {
            val.publishTo(out);
        }
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int pushSize = readTerminatedInteger(in);
        value = new IRESP[pushSize];
        for( int i = 0; i < pushSize; ++i ) {
            value[ i ] = factory.getNext(in);
        }

        return false;
    }

    public RESPPush(IRESPFactory factory, InputStream in) throws java.io.IOException {
        readFrom(in,factory);
    }

    public boolean resp3 = false;
    //For subscribe/unsubscribe
    public RESPPush(boolean resp3, RESPBulkString kind, RESPBulkString channelName, RESPInteger index) {
        this.resp3 = resp3;
        value = new IRESP [] {kind,channelName,index};
    }

    //For messages
    public RESPPush(boolean resp3, RESPBulkString kind, RESPBulkString channelName, RESPBulkString message) {
        this.resp3 = resp3;
        value = new IRESP [] {kind,channelName,message};
    }

    @Override
    public String toString() {
        if( value == null ) return "RESPPush<null>";
        StringBuilder sb = new StringBuilder();
        sb.append("RESPPush<");
        String prefix = "";
        for( IRESP i : value ) {
            sb.append(prefix);
            if( i == null ) sb.append("null");
            else sb.append(i.toString());
            prefix=",";
        }
        sb.append(">");
        return sb.toString();
    }

    public IRESP [] value;
    public static final char resp2Prefix = '*';

    public static final char prefix = '>';
}
