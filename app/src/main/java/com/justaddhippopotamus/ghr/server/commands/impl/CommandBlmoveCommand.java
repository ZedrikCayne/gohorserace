
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBlmoveCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
        RESPArrayScanner commands = item.scanner();
        String source = commands.key();
        String destination = commands.key();
        boolean sourceLEFT = commands.argIsRequired("RIGHT","LEFT");
        boolean destinationLEFT = commands.argIsRequired( "RIGHT", "LEFT" );
        double timeout = commands.takeDouble();
        RedisList sourceList = item.getMainStorage().fetchRW(source,RedisList.class,RedisList::new);
        sourceList.atomic( (RedisList l) -> {
            if( item.timedOut() ) {
                item.whoFor.queueNullBulkString(item.order);
                Utils.setUnblocked(item,l);
            }
            if( !l.isEmpty() ) {
                RedisList destinationList = item.getMainStorage().fetchRW(destination,RedisList.class,RedisList::new);
                item.whoFor.queue(sourceList.move(destinationList,sourceLEFT,destinationLEFT),item.order);
                Utils.setUnblocked(item,l);
            }
            Utils.setBlocking(item,l,timeout);
        });

    }
}
