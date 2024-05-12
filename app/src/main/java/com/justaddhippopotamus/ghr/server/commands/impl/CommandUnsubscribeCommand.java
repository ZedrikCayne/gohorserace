
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandUnsubscribeCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        List<String> channels = commands.remainingElementsOptional();
        if( channels.isEmpty() ) {
            item.whoFor.unsubscribeAll();
        } else {
            for (String channel : channels) {
                item.whoFor.unsubscribeTo(item.getServer().channelManager().getChannel(channel));
            }
        }
        item.whoFor.queueNop(item.order);
    }
}
