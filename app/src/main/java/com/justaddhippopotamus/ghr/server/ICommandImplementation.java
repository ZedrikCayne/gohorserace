package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.RESPArray;

import java.util.*;

//The super class of all implementations of commands.
public abstract class ICommandImplementation {
    public abstract void runCommand(WorkItem item);
    public List<String> parseCommand(RESPArray commands) {
        List<String> returnValue = new ArrayList<>(commands.value.size());
        commands.value.stream().forEach( x -> {
            returnValue.add(x.toString());
        });
        return returnValue;
    }
}
