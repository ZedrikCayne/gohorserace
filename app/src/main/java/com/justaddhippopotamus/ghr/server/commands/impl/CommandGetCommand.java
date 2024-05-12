
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGetCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        item.whoFor.queue(item.getMainStorage().fetchRO(key, RedisString.class),item.order);
    }
}
