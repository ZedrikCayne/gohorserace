package com.justaddhippopotamus.ghr.RESP;

import com.justaddhippopotamus.ghr.server.Server;

import java.io.InputStream;
import java.io.OutputStream;

public class RESPVerbatimString extends IRESP {
    @Override
    public boolean publishTo(OutputStream out) throws java.io.IOException {
        byte [] data = value.getBytes(Server.CHARSET);
        byte [] encodingToSend = encoding.getBytes(Server.CHARSET);

        if( encodingToSend.length != 3 )
            throwIOException("Encoding length is not 3: " + encoding);
        int totalLength  = data.length + encodingToSend.length + 1; //+1 for the :
        writeTerminatedInteger(totalLength,out);
        out.write(encodingToSend);
        out.write(':');
        out.write(data);
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws java.io.IOException {
        int lengthAsInt = readTerminatedInteger(in);
        byte [] data = in.readNBytes(lengthAsInt);

        if( data.length != lengthAsInt )
            throwIOException("Not enough bytes to read.");

        encoding = new String(data,0,3, Server.CHARSET);

        if( data[3] != ':' )
            throwIOException("Missing : between encoding and data.");

        value = new String(data, 4, lengthAsInt - 4, Server.CHARSET);

        return false;
    }

    public String encoding = null;
    public String value = null;

    public RESPVerbatimString(InputStream in) throws java.io.IOException {
        readFrom(in,null);
    }

    public RESPVerbatimString(String in) {
        value = in;
        encoding = "txt";
    }

    @Override
    public String toString() {
        return encoding + ":" + value;
    }

    public static final char prefix = '=';
}
