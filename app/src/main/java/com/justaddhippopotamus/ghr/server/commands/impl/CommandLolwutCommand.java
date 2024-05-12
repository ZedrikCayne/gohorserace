
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPVerbatimString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLolwutCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        String lolwhut = "        ,--,\n" +
                         "  _ ___/ /\\|\n" +
                         " ;( )__, )\n" +
                         "; //   '--;\n" +
                         "  \\     |\n" +
                         "   ^    ^\n" +
                         "GoHorseRedis (Redis 7.2 compatible) V0.1";
        item.whoFor.queue(new RESPVerbatimString(lolwhut),item.order);
    }
}
