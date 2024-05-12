
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
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
        int count = 0;
        if(COUNT) count = arguments.takeInt();
        for( String key : keys ) {
            RedisSortedSet rs = item.getMainStorage().fetchRW(key, RedisSortedSet.class);
            if( rs != null ) {
                item.whoFor.queue(RESPArray.fromListOfSortedSetValues(MAX?rs.popmax(count):rs.popmin(count)),item.order);
                return;
            }
        }
        item.whoFor.queueNullBulkString(item.order);
    }
}
