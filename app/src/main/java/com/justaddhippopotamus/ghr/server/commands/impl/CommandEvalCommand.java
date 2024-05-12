
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.LuaHandler;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandEvalCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        genericCommand(item);
    }
    public static void genericCommand(WorkItem item) {
        //EVAL script numkeys [key [key ...]] [arg [arg ...]]
        final RESPArrayScanner commands = item.scanner();
        String script = commands.string();
        genericRunner(item, commands, script);
    }

    public static void genericRunner(WorkItem item, RESPArrayScanner commands, String script) {
        List<String> keys = commands.getNumKeys();
        final List<RESPBulkString> args = commands.remainingBulkStrings();
        LuaHandler handler = new LuaHandler(item.whoFor);
        item.whoFor.queue(handler.eval(item.whoFor,script,keys,args),item.order);
    }
}
