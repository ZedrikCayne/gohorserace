
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandDiscardCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        if( item.whoFor.isMulti() ) {
            item.whoFor.discardMulti();
            item.whoFor.queueOK(item.order);
        } else {
            item.whoFor.queueSimpleError("We were not in a transaction.",item.order);
        }
    }
}
