
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSrandmemberCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SRANDMEMBER key [count]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        boolean  hasCount = commands.hasNext();
        int count = hasCount?commands.takeInt():1;
        RedisSet rs = item.getMainStorage().fetchRO(key,RedisSet.class);
        if( rs == null ) {
            if( hasCount ) item.whoFor.queueEmptyArray(item.order);
            else item.whoFor.queueNullBulkString(item.order);
        } else {
            List<String> returnValue = rs.rand(count);
            if( hasCount ) item.whoFor.queue(returnValue,item.order);
            else {
                if(returnValue.isEmpty()) item.whoFor.queueNullBulkString(item.order);
                else item.whoFor.queueBulkString(returnValue.get(0),item.order);
            }
        }
    }
}
