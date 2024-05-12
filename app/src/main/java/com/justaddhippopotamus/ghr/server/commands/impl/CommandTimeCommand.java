
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.time.Instant;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandTimeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        Instant now = Instant.now();
        RESPArray returnValue = new RESPArray(2);
        returnValue.addRespElement(new RESPBulkString(String.valueOf(now.getEpochSecond())));
        returnValue.addRespElement(new RESPBulkString(String.valueOf(now.getNano())));
        item.whoFor.queue(returnValue,item.order);
    }
}
