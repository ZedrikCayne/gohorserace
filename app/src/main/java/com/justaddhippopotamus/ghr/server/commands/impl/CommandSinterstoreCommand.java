
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSet;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSinterstoreCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RedisSet toStore = CommandSinterCommand.genericCommand(item);
        item.getMainStorage().store(item.what.stringAt(1),toStore);
        item.whoFor.queueInteger(toStore.size(),item.order);
    }
}
