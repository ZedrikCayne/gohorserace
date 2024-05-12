
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.*;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandRenameCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String sourceKey = commands.key();
        String destinationKey = commands.key();
        commands.errorOnRemains();

        long returnValue = item.getMainStorage().rename(sourceKey,destinationKey,false);
        if( returnValue == TypeStorage.RENAME_FAIL_NO_SOURCE )
            item.whoFor.queueSimpleError("No such key", item.order);
        else
            item.whoFor.queueOK(item.order);
    }
}
