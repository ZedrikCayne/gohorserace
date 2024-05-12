
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandKeyParser;
public class KeySearchFunctionGetKeys extends ICommandKeyParser {
    @Override
    public String[] processKeys(RESPArray command) {
        return new String[0];
    }
}
