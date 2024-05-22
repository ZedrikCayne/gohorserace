
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandLposCommand extends ICommandImplementation {
    //LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
    @Override
    public void runCommand(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        var key = commands.key();
        var element = commands.bulkString();
        int rank = 1;
        if( commands.argIs("RANK") ) {
            rank = commands.takeInt();
        }
        boolean hasCount = commands.argIs("COUNT");
        int count = 1;
        if(hasCount) {
            count = commands.takeInt();
        }
        int maxLen = 0;
        if( commands.argIs("MAXLEN") ) {
            maxLen = commands.takeInt();
        }
        RedisList rl = item.getMainStorage().fetchRO(key, RedisList.class);
        if( rl == null ) {
            item.whoFor.queueNullArray(item.order);
        } else {
            List<Integer> posResult = rl.pos(element,count,maxLen,rank);
            if( posResult.isEmpty() ) {
                item.whoFor.queueNullArray(item.order);
                return;
            }
            if( hasCount ) {
                item.whoFor.queue(RESPArray.RESPArrayIntegers(posResult),item.order);
            } else {
                item.whoFor.queueInteger(posResult.get(0),item.order);
            }
        }
    }
}
