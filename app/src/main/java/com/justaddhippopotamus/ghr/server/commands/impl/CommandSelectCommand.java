
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSelectCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        item.whoFor.select(item.what.intAt(1));
        item.whoFor.queueOK(item.order);
    }
}
