
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLrangeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int start = commands.takeInt();
        int stop = commands.takeInt();
        commands.errorOnRemains();
        RedisList rl = item.getMainStorage().fetchRO(key,RedisList.class);
        if( rl == null ) {
            item.whoFor.queueEmptyArray(item.order);
        } else {
            item.whoFor.queue(rl.range(start,stop),item.order);
        }
    }
}
