
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandRpoplpushCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String source = commands.key();
        String destination = commands.key();
        commands.errorOnRemains();
        RedisList rl = item.getMainStorage().fetchRW(source,RedisList.class);
        if( RedisList.isNullOrEmpty(rl) ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            RedisList dl = item.getMainStorage().fetchRW(destination,RedisList.class, RedisList::new);
            item.whoFor.queueBulkString(rl.move(dl,false,true),item.order);
            dl.unqueueAll(item.getServer());
        }
    }
}
