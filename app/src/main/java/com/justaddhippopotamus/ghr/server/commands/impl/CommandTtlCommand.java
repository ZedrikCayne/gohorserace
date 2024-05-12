
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisType;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandTtlCommand extends ICommandImplementation {
    public static final long KEY_NOT_EXIST = -2;
    public static final long KEY_NO_EXPIRE = -1;

    public static void genericExpire(Client client, String key, long order, boolean milliseconds, boolean relative) {
        RedisType rt = client.getMainStorage().fetchRW(key,RedisType.class);
        if( RedisType.isNullOrExpired(rt) ) {
            client.queueInteger(KEY_NOT_EXIST,order);
        } else {
            if (rt.isPersistent()) {
                client.queueInteger(KEY_NO_EXPIRE,order);
            } else {
                long realTime = rt.getExpireAtMilliseconds();
                if( relative ) realTime -= System.currentTimeMillis();
                if( !milliseconds ) realTime = realTime / 1000L;
                client.queueInteger(realTime,order);
            }
        }

    }
    @Override
    public void runCommand(WorkItem item) {
        genericExpire(item.whoFor, item.what.stringAt(1),item.order, false,true);
    }
}
