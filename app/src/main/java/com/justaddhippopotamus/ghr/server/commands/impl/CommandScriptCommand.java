
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandScriptCommand extends ICommandImplementation {
    private void exists(WorkItem item, RESPArrayScanner commands) {
        List<String> sha1 = commands.remainingElementsRequired(0 );
        item.whoFor.queue( RESPArray.RESPArrayIntegers(item.getServer().luaStorage().exists(sha1)),
                item.order);
    }

    private void debug(WorkItem item, RESPArrayScanner commands) {
        item.whoFor.queueOK(item.order);
    }

    private void load(WorkItem item, RESPArrayScanner commands) {
        item.whoFor.queue(new RESPBulkString(item.getServer().luaStorage().load(commands.string())),item.order);
    }

    private void kill(WorkItem item, RESPArrayScanner commands) {

    }

    private void flush(WorkItem item, RESPArrayScanner commands) {
        boolean ASYNC = commands.argOneOf("ASYNC","SYNC").equals("ASYNC");
        item.getServer().luaStorage().flush(ASYNC);
        item.whoFor.queueOK(item.order);
    }
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner(true);
        String subcommand = commands.subcommand();
        switch( subcommand ) {
            case "DEBUG":
                debug(item,commands);
                return;
            case "EXISTS":
                exists(item,commands);
                break;
            case "FLUSH":
                flush(item,commands);
                break;
            case "KILL":
                kill(item,commands);
                return;
            case "LOAD":
                load(item,commands);
                break;
            default:
                commands.throwError();
                break;
        }

    }
}
