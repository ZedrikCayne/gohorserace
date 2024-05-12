
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLlenCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        RedisList rl = item.getMainStorage().fetchRO(key,RedisList.class);
        if( rl == null )
            item.whoFor.queueInteger(0,item.order);
        else
            item.whoFor.queueInteger(rl.size(),item.order);
    }
}
