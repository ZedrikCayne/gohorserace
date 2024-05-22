
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBrpoplpushCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //BRPOPLPUSH source destination timeout
        RESPArrayScanner commands = item.scanner();
        String source = commands.key();
        String destination = commands.key();
        double timeout = commands.takeDouble();
        RedisList sourceList = item.getMainStorage().fetchRW(source, RedisList.class,RedisList::new);
        RedisList destinationList = item.getMainStorage().fetchRW(destination,RedisList.class,RedisList::new);
        sourceList.atomic( (RedisList l) -> {
            if( item.timedOut() ) {
                item.whoFor.queueNullBulkString(item.order);
                Utils.setUnblocked(item,l);
                return;
            }
            if( !sourceList.isEmpty() ) {
                item.whoFor.queue(sourceList.move(destinationList,false,true),item.order);
                Utils.setUnblocked(item,l);
                return;
            }
            Utils.setBlocking(item,l,timeout);
        });
    }
}
