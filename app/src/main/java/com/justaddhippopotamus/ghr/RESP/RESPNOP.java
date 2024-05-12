package com.justaddhippopotamus.ghr.RESP;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

//Stupid NOP implementation so clients can keep the indexes for last
//instructions in line. (We kinda guarantee that commands you send
//will be answered in the order you send them, even if they are completed
//out of order.
public class RESPNOP extends IRESP{
    @Override
    public boolean publishTo(OutputStream out) throws IOException {
        return false;
    }

    @Override
    public boolean readFrom(InputStream in, IRESPFactory factory) throws IOException {
        return false;
    }
}
