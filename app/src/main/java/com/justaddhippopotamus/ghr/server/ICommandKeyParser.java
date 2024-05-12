package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPArray;

public abstract class ICommandKeyParser {
    public abstract String [] processKeys(RESPArray command);
}
