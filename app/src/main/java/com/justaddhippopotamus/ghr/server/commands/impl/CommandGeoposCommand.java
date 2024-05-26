
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.RESP.RESPBulkString;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeoposCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //GEOPOS key [element ...]
        var commands = item.scanner();
        var key = commands.key();
        var elements = commands.remainingElementsOptional();
        int len = elements.size();
        var gh = item.getMainStorage().fetchRO(key, RedisGeo.class);
        RESPArray ra = new RESPArray(elements.size());
        if( gh == null ) {
            for( int i = 0; i < len; ++i )
                ra.addRespElement(item.whoFor.clientRESPVersion.nullFor());
        } else {
            for( var e : elements ) {
                Double val = gh.getScore(e);
                if( val == null ) {
                    ra.addRespElement(item.whoFor.clientRESPVersion.nullFor());
                } else {
                    double [] xy = {0,0};
                    RedisGeo.degeohash(val,xy);
                    RESPArray latlong = new RESPArray(2);
                    latlong.addString(Utils.doubleToStringRedisStyle(xy[RedisGeo.XYLONG]));
                    latlong.addString(Utils.doubleToStringRedisStyle(xy[RedisGeo.XYLAT]));
                    ra.addRespElement(latlong);
                }
            }
        }
        item.whoFor.queue(ra,item.order);
    }
}
