
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandPubsubCommand extends ICommandImplementation {

    private void channels(WorkItem item, RESPArrayScanner commands) {
        List<String> channels = item.getServer().channelManager().channelNames();
        if( commands.hasNext() ) {
            String pattern = commands.string();
            item.whoFor.queue(Utils.getMatches(channels,pattern),item.order);
        } else {
            item.whoFor.queue(item.getServer().channelManager().channelNames(),item.order);
        }
    }

    private void numpat(WorkItem item, RESPArrayScanner scanner) {
        item.whoFor.queueOK(item.order);
    }

    private void shardchannels(WorkItem item, RESPArrayScanner scanner) {
        item.whoFor.queueOK(item.order);
    }

    private void shardnumsub(WorkItem item, RESPArrayScanner scanner) {
        item.whoFor.queueOK(item.order);
    }

    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner(true);
        switch( commands.subcommand() ) {
            case "CHANNELS":
                break;
            case "NUMPAT":
                break;
            case "SHARDCHANNELS":
                break;
            case "SHARDNUMSUB":
                break;
        }

    }
}
