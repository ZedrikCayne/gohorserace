
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;
import com.justaddhippopotamus.ghr.server.types.RedisSet;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandSortCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern
        //  ...]] [ASC | DESC] [ALPHA] [STORE destination]
        RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        String pattern = commands.argIs("BY")?commands.string():null;
        int offset = 0;
        int count = -1;
        if( commands.argIs("LIMIT") ) {
            offset = commands.takeInt();
            count = commands.takeInt();
        }
        List<String> patterns = new ArrayList<>();
        while( commands.argIs("GET") ) {
            patterns.add( commands.string() );
        }
        boolean DESC = commands.argIs("DESC");
        commands.argIs("ASC");
        boolean ALPHA = commands.argIs("ALPHA");
        String destination = commands.argIs("STORE")?commands.string():null;
        RedisType sortTarget = item.getMainStorage().fetchRO(key,RedisType.class);
        if( sortTarget instanceof RedisList) {

        } else if (sortTarget instanceof RedisSet) {

        } else if (sortTarget instanceof RedisSortedSet) {

        }
        item.whoFor.queueOK(item.order);
    }

    private static class SortItem {
        String sVal;
        Double dVal;
    }
}
