
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandMoveCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //MOVE source db
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        int db = commands.takeInt();
        item.whoFor.queueInteger(item.getMainStorage().move(key,db),item.order);
    }

}
