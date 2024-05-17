
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisList;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBlpopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        genericBPop(item);
    }
    public static void genericBPop(WorkItem item) {
        //BLPOP keys [key ... ] timeout
        //BRPOP keys [key ....] timeout
        RESPArrayScanner commands = item.scanner();
        boolean left = commands.commandIs("BLPOP");

        List<String> keys = commands.remainingElementsRequired(-1);
        double timeout = commands.takeDouble();
        List<RedisList> rList = item.getMainStorage().fetchRWMany(keys,RedisList.class,RedisList::new);
        RedisType.atomicAllStatic( rList, Boolean.class, all -> {
            if( item.timedOut() ) {
                item.whoFor.queueNullArray(item.order);
                Utils.setUnblocked(item,all);
                return false;
            }
            int index = 0;
            for( var l : all ) {
                if (l.size() > 0) {
                    RESPArray queueValue = new RESPArray(2);
                    queueValue.addString(keys.get(index));
                    if( left )
                        queueValue.addString(l.pop());
                    else
                        queueValue.addString(l.rpop());
                    item.whoFor.queue(queueValue, item.order);
                    Utils.setUnblocked(item,all);
                    return false;
                }
                ++index;
            }
            Utils.setBlocking(item,all,timeout);
            return true;
        } );
    }
}
