
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZcardCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        RedisSortedSet ss = item.getMainStorage().fetchRO(key,RedisSortedSet.class);
        if( ss == null )
            item.whoFor.queueInteger(0,item.order);
        else
            item.whoFor.queueInteger( ss.size(), item.order );
    }
}
