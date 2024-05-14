
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandExecCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        if( item.whoFor.isMulti() ) {
            List<RESPArray> queued = item.whoFor.queuedStuff;
            item.whoFor.queuedStuff = new ArrayList<>();
            item.whoFor.startExecute();
            item.getServer().executeMany(queued,item.whoFor,item.order);
            item.whoFor.execute(item.order);
        } else {
            item.whoFor.queueSimpleError("Not in a transaction.", item.order);
        }
    }
}
