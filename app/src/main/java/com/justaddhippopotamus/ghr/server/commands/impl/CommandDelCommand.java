
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandDelCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        int returnValue = 0;
        while( commands.hasNext() ) {
            String key = commands.key();
            if( item.getMainStorage().del(key) )
                ++returnValue;
        }
        item.whoFor.queueInteger(returnValue,item.order);
    }
}