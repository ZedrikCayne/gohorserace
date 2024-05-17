
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBlmpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
        RESPArrayScanner commands = item.scanner();
        double timeout = commands.takeDouble();
        List<String> keys = commands.getNumKeys();
        boolean LEFT = commands.argIsRequired("RIGHT", "LEFT");
        int count = commands.argIs("COUNT")?commands.takeInt():1;
        List<RedisList> elements = item.getMainStorage().fetchRWMany(keys,RedisList.class,RedisList::new);
        RedisType.atomicAllStatic(elements,Boolean.class, all -> {
            if( item.timedOut() ) {
                item.whoFor.queueNullBulkString(item.order);
                Utils.setUnblocked(item,all);
                return false;
            }
            int index = 0;
            for( var l : all ) {
                if( !l.isEmpty() ) {
                    RESPArray queueValue = new RESPArray(2);
                    queueValue.addString( keys.get(index));
                    queueValue.addRespElement( new RESPArray(LEFT?l.pop(count):l.rpop(count)));
                    item.whoFor.queue(queueValue,item.order);
                    Utils.setUnblocked(item,all);
                    return false;
                }
                ++index;
            }
            Utils.setBlocking(item,all,timeout);
            return true;
        });
    }
}
