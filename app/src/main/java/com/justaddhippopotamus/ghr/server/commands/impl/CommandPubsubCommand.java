
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
            item.whoFor.queueStrings(Utils.getMatches(channels,pattern),item.order);
        } else {
            item.whoFor.queueStrings(item.getServer().channelManager().channelNames(),item.order);
        }
    }

    private void numsub(WorkItem item, RESPArrayScanner commands) {
        List<String> channels = commands.remainingElementsRequired(0);
        RESPArray returnValue = new RESPArray(channels.size() * 2);
        for(var channel:channels) {
            returnValue.addString(channel);
            returnValue.addInteger(item.getServer().channelManager().getChannel(channel).numClients());
        }
        item.whoFor.queue(returnValue,item.order);
    }

    private void numpat(WorkItem item, RESPArrayScanner scanner) {
        item.whoFor.queueInteger(item.getServer().channelManager().numPatterns(),item.order);
    }

    private void shardchannels(WorkItem item, RESPArrayScanner scanner) {
        item.whoFor.queueEmptyArray(item.order);
    }

    private void shardnumsub(WorkItem item, RESPArrayScanner scanner) {
        item.whoFor.queueInteger(0, item.order);
    }

    @Override
    public void runCommand(WorkItem item) {
        RESPArrayScanner commands = item.scanner(true);
        switch( commands.subcommand() ) {
            case "CHANNELS":
                channels(item,commands);
                break;
            case "NUMSUB":
                numsub(item,commands);
                break;
            case "NUMPAT":
                numpat(item,commands);
                break;
            case "SHARDCHANNELS":
                shardchannels(item,commands);
                break;
            case "SHARDNUMSUB":
                shardnumsub(item,commands);
                break;
            default:
                item.whoFor.queueSimpleError("I don't get: " + item.toString(), item.order);
        }

    }
}
