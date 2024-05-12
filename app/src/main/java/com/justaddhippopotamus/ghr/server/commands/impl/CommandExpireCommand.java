
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisType;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandExpireCommand extends ICommandImplementation {
    public static final int TIMEOUT_NOT_SET = 0;
    public static void genericExpireCommand(RESPArray commands, Client client, long order) {
        long multiplier = 1;
        long add = System.currentTimeMillis();
        String baseCommand = commands.argAt(0);
        switch(baseCommand) {
            case "EXPIREAT":
                add = 0;
            case "EXPIRE":
                multiplier = 1000;
                break;
            case "PEXPIREAT":
                add = 0;
            case "PEXPIRE":
                multiplier = 1;
            default:
                throw new RuntimeException("Bad argument for expire command.");
        }
        String key = commands.stringAt(1);
        long time = commands.longAt( 2 );
        boolean NX = false, XX = false, GT = false, LT = false;
        if( commands.size() > 3 ) {
            String arg = commands.argAt(3);
            switch (arg) {
                case "NX":
                    NX = true;
                    break;
                case "XX":
                    XX = true;
                    break;
                case "GT":
                    GT = true;
                    break;
                case "LT":
                    LT = true;
                    break;
                default:
                    throw new RuntimeException("Bad argument on EXPIRE");
            }
        }
        RedisType rt = client.getMainStorage().fetchRW(key, RedisType.class);
        if (RedisType.isNullOrExpired(rt)) {
            client.queueInteger(TIMEOUT_NOT_SET,order);
        } else {
            client.queueInteger(rt.setExpireMilliseconds(time * multiplier + add, NX, XX, GT, LT),order);
        }
    }

    //EXPIRE key seconds [NX | XX | GT | LT]
    @Override
    public void runCommand(WorkItem item) {
        genericExpireCommand(item.what,item.whoFor,item.order);
    }
}
