
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZremCommand extends ICommandImplementation {
    //ZREM key member [member ...]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<String> thingsToRemove = commands.remainingElementsRequired(0);
        commands.errorOnRemains();
        RedisSortedSet rss = item.getMainStorage().fetchRW(key, RedisSortedSet.class);
        if( rss == null ) {
            item.whoFor.queueInteger(0, item.order);
        } else {
            item.whoFor.queueInteger(rss.removeAll(thingsToRemove),item.order);
        }
    }
}
