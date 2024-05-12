
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZrangestoreCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        CommandZrangeCommand.zrangeGenericCommandAndQueue(item.what,item.whoFor,item.order,2,false,false,false, false, item.what.stringAt(1));
    }
}
