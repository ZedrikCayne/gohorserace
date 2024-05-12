
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPexpireatCommand extends ICommandImplementation {
    public static final int TIMEOUT_NOT_SET = 0;
    //PEXPIREAT key milliseconds [NX | XX | GT | LT]
    @Override
    public void runCommand(WorkItem item) {
        CommandExpireCommand.genericExpireCommand(item.what,item.whoFor,item.order);
    }
}
