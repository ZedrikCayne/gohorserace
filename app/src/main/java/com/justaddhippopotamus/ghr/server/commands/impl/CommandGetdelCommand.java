
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGetdelCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String key = item.what.stringAt(1);
        RedisString rs = item.getMainStorage().fetchDel(key, RedisString.class);
        item.whoFor.queue(rs,item.order);
    }
}
