
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeohashCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //GEOHASH key [element ...]
        var commands = item.scanner();
        var key = commands.key();
        var elements = commands.remainingElementsOptional();
        var gh = item.getMainStorage().fetchRO(key, RedisGeo.class);
        if( gh == null ) {
            item.whoFor.queueNullBulkString(item.order);
        } else {
            RESPArray ra = new RESPArray(elements.size());
            elements.forEach(e -> {
                Double v = gh.getScore(e);
                if( v == null ) {
                    ra.addString("");
                } else {
                    ra.addRespElement(new RESPBulkString(RedisGeo.geoHashToBytes(RedisGeo.geohashReal(v))));
                }
            });
            item.whoFor.queue(ra,item.order);
        }
    }
}
