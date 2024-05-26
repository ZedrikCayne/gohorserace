
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeodistCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //GEODIST key member1 member2 [M | KM | FT | MI]
        var commands = item.scanner();
        var key = commands.key();
        var member1 = commands.key();
        var member2 = commands.key();
        var unit = commands.argOneOf("M","KM","FT","MI");
        var gh = item.getMainStorage().fetchRO(key, RedisGeo.class);
        var val = gh.distance(member1,member2,unit);
        if( val < 0 )
            item.whoFor.queueNullBulkString(item.order);
        else
            item.whoFor.queueBulkString(Utils.doubleToStringRedisGeoStyle(val),item.order);
    }
}
