
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandAuthCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner scanner = item.scanner();
        String username = null;
        String password = scanner.string();
        if( scanner.hasNext() ) {
            username = password;
            password = scanner.string();
        }
        if( item.getServer().passwordValid(username,password) ) {
            item.whoFor.auth();
            item.whoFor.queueOK(item.order);
        } else {
            item.whoFor.queueSimpleError("Username/password incorrect.",item.order);
        }
    }
}
