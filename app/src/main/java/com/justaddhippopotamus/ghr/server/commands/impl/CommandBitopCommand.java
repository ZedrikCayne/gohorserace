
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisString;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.List;
import java.util.Objects;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandBitopCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //BITOP <AND | OR | XOR | NOT> destkey key [key ...]
        final RESPArrayScanner commands = item.scanner();
        String operation = commands.argOneOfRequired("AND","OR","XOR","NOT");
        String dest = commands.key();
        List<String> sources = commands.remainingElementsRequired(0);
        RedisString returnValue;
        if(operation.compareTo("NOT") == 0) {
            if( sources.size() > 1 ) {
                item.whoFor.queueSimpleError("BITOP NOT only takes one key.", item.order);
                return;
            }
            else
            {
                RedisString source = item.getMainStorage().fetchRO(sources.get(0),RedisString.class);
                if( source == null) {
                    returnValue = new RedisString();
                } else {
                    returnValue = source.not();
                }
            }
        } else {
            List<RedisString> rs = item.getMainStorage().mgetString(sources);
            returnValue = RedisType.atomicAllStatic(rs, RedisString.class, allStrings -> {
                int maxString = allStrings.stream().filter(Objects::nonNull).mapToInt(RedisString::length).max().orElse(-1);
                if (maxString < 0) {
                    return new RedisString();
                }
                RedisString result = new RedisString(maxString);
                result.setBitOp(allStrings.get(0));
                for( int i = 1; i < allStrings.size(); ++i ) {
                    RedisString other = allStrings.get(i);
                    switch (operation) {
                        case "AND":
                            result.and(other);
                            break;
                        case "OR":
                            result.or(other);
                            break;
                        case "XOR":
                            result.xor(other);
                            break;
                        default:
                            item.whoFor.queueSimpleError("I don't understand the operation " + operation, item.order);
                            return null;
                    }
                }
                return result;
            });
        }
        item.getMainStorage().store(dest, returnValue);





    }
}
