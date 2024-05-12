
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.LuaHandler;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandEvalShaCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        genericCommand(item);

    }

    public static void genericCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        String script = item.getServer().luaStorage().getBySHA1(commands.string());
        if( script == null ) {
            item.whoFor.queueSimpleError("Script not in cache.", item.order);
        } else {
            CommandEvalCommand.genericRunner(item, commands, script);
        }
    }
}
