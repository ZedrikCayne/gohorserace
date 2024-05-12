
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPunsubscribeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        List<String> patterns = commands.remainingElementsOptional();
        if( patterns.isEmpty() ) {
            item.whoFor.unsubscribeAllPatterns();
        } else {
            patterns.forEach(c -> item.whoFor.unsubscribeToPattern(item.getServer().channelManager().getPattern(c)));
        }
        item.whoFor.queueNop(item.order);
    }
}
