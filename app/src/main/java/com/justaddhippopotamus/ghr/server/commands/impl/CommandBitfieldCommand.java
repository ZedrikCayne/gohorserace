
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.*;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBitfieldCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        genericCommand(item);
    }
    public static void genericCommand(WorkItem item) {
        //BITFIELD key [GET encoding offset | [OVERFLOW <WRAP | SAT | FAIL>]   <SET encoding offset value | INCRBY encoding offset increment> [GET encoding offset | [OVERFLOW <WRAP | SAT | FAIL>] <SET encoding offset value | INCRBY encoding offset increment> ...]]
        final RESPArrayScanner commands = item.scanner();
        String key = commands.key();
        List<RedisString.BitFieldOp> operations = new ArrayList<>();
        RedisString.BitFieldOverflowMode currentMode = RedisString.BitFieldOverflowMode.WRAP;
        while( commands.hasNext() ) {
            final String command = commands.argOneOfRequired("GET","OVERFLOW","SET","INCRBY");
            switch(command) {
                case "GET":
                    operations.add(new RedisString.BitFieldGet(commands.string(),commands.string()));
                    break;
                case "OVERFLOW":
                    currentMode = RedisString.modeForString(commands.argOneOfRequired("WRAP","SAT","FAIL"));
                    break;
                case "SET":
                    operations.add(new RedisString.BitFieldSet(commands.string(),commands.string(),commands.takeInt(),currentMode));
                    break;
                case "INCRBY":
                    operations.add(new RedisString.BitFieldIncr(commands.string(),commands.string(),commands.takeInt(),currentMode));
                    break;
            }
        }
        commands.errorOnRemains();
        RedisString rs = item.getMainStorage().fetchRW(key,RedisString.class,RedisString::new);
        rs.atomic( (RedisString operateOn) -> {
            List<Long> results = operations.stream().map( operation -> operation.operateOn(operateOn)).collect(Collectors.toList());
            item.whoFor.queue(new RESPArray(results.stream().map( aLong -> {
                if( aLong == null ) return item.whoFor.clientRESPVersion == IRESP.RESPVersion.RESP2 ? Client.NIL_BULK_STRING : Client.NULL;
                return new RESPInteger(aLong);
            }).collect(Collectors.toList())),item.order);
        });
    }
}
