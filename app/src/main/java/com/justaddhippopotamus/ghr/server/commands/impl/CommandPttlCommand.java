
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPttlCommand extends ICommandImplementation {
    public static final long KEY_NOT_EXIST = -2;
    public static final long KEY_NO_EXPIRE = -1;
    @Override
    public void runCommand(WorkItem item) {
        CommandTtlCommand.genericExpire(item.whoFor, item.what.stringAt(1), item.order, true, true);
    }
}
