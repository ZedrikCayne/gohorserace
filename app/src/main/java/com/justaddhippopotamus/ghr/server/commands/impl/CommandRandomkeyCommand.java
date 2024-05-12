
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandRandomkeyCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //RANDOMKEY
        String randKey = item.getMainStorage().randkey();
        if( randKey == null )
            item.whoFor.queueNullBulkString(item.order);
        else
            item.whoFor.queue(new RESPBulkString(randKey),item.order);
    }
}
