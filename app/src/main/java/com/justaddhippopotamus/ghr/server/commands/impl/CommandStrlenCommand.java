
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandStrlenCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //STRLEN key
        RedisString rs = item.getMainStorage().fetchRO(item.what.stringAt(1),RedisString.class);
        item.whoFor.queueInteger(rs==null?0:rs.length(),item.order);
    }
}
