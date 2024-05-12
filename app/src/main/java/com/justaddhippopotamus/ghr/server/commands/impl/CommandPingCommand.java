
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPingCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        if( item.what.size() > 1 ) {
            item.whoFor.queue(item.what.rbsAt(1),item.order);
        } else {
            item.whoFor.queueSimpleString("PONG",item.order);
        }
    }
}
