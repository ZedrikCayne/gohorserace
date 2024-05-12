
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLindexCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int index = commands.takeInt();
        commands.errorOnRemains();

        RedisList rl = item.getMainStorage().fetchRO(key,RedisList.class);
        if( rl == null ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            item.whoFor.queue( new RESPBulkString( rl.index(index) ), item.order);
        }
    }
}
