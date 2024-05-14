
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisString;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZmpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
        RESPArrayScanner arguments = item.scanner();
        List<String> keys = arguments.getNumKeys();
        boolean MAX = arguments.argIsRequired("MIN","MAX");
        boolean COUNT = arguments.argIs("COUNT");
        int count = 1;
        if(COUNT) count = arguments.takeInt();
        for( String key : keys ) {
            RedisSortedSet rs = item.getMainStorage().fetchRW(key, RedisSortedSet.class);
            if( rs != null ) {
                List<RedisSortedSet.SetValue> sv = MAX?rs.popmax(count):rs.popmin(count);
                if( !sv.isEmpty() ) {
                    item.whoFor.queue(listOfListsOfSortedLists(key,sv),item.order);
                    return;
                }
            }
        }
        item.whoFor.queueNullBulkString(item.order);
    }


    private RESPArray listOfListsOfSortedLists( String key, List<RedisSortedSet.SetValue> values ) {
        RESPArray returnValue = new RESPArray(2);
        returnValue.addRespElement(new RESPBulkString(key));
        RESPArray outer = new RESPArray(values.size());
        returnValue.addRespElement(outer);
        for( var sv : values ) {
            RESPArray inner = new RESPArray(2);
            inner.addRespElement(new RESPBulkString(sv.key));
            inner.addRespElement(new RESPBulkString(sv.score));
            outer.addRespElement(inner);
        }
        return returnValue;
    }
}
