
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandExistsCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        int len = item.what.size();
        int returnValue = 0;
        for( int i = 1; i < len; ++i ) {
             if(item.getMainStorage().keyExists(item.what.stringAt(i)))
                 ++returnValue;
        }
        item.whoFor.queueInteger(returnValue,item.order);
    }
}
