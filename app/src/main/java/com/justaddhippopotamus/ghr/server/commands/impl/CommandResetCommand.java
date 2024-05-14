
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPSimpleString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandResetCommand extends ICommandImplementation {
    private static RESPSimpleString RESET = new RESPSimpleString("RESET");
    @Override
    public void runCommand(WorkItem item) {
        item.whoFor.reset();
        item.whoFor.queue(RESET,1);
    }
}
