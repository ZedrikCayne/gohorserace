
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandRpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //RPOP key [count]
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int count = 1;
        boolean hasCount = false;
        if( commands.hasNext() ) {
            count = commands.takeInt();
            hasCount = true;
        }
        commands.errorOnRemains();
        RedisList rl = item.getMainStorage().fetchRO(key,RedisList.class);
        if( rl == null ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            List<RESPBulkString> rv = rl.rpop(count);
            if( hasCount ) {
                item.whoFor.queue(rv,item.order);
            } else {
                item.whoFor.queue(rv.get(0),item.order);
            }
        }
    }
}
