
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLmoveCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //LMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT>
        final RESPArrayScanner commands = item.scanner();
        String source = commands.key();
        String destination = commands.key();
        boolean LEFTFROM = commands.argIsRequired("RIGHT", "LEFT" );
        boolean LEFTTO = commands.argIsRequired( "RIGHT", "LEFT");
        RedisList sourceList = item.getMainStorage().fetchRW(source, RedisList.class);
        if( sourceList == null ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            RedisList destinationList = sourceList;
            if( destination.compareTo(source) != 0 ) {
                destinationList = item.getMainStorage().fetchRW(destination,RedisList.class, RedisList::new);
            }
            item.whoFor.queue(sourceList.move(destinationList,LEFTFROM,LEFTTO),item.order);
            destinationList.unqueueAll(item.getServer());
        }
    }
}
