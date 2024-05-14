
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandTouchCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //TOUCH key [key ...]
        RESPArrayScanner scanner = item.scanner();
        List<String> keys = scanner.remainingElementsRequired(0);
        item.whoFor.queueInteger(item.getMainStorage().touch(keys),item.order);
    }
}
