
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSmismemberCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SMISMEMBER key member [member ...]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<String> members = commands.remainingElementsRequired(0);
        int len = members.size();
        RedisSet set = item.getMainStorage().fetchRO(key, RedisSet.class);
        RESPArray returnValue;
        if( set == null ) {
            returnValue = new RESPArray(len);
            for( int i = 0; i < len; ++i ) returnValue.addInteger(0);
        } else {
            returnValue = set.sismember(members);
        }
        item.whoFor.queue(returnValue,item.order);

    }
}
