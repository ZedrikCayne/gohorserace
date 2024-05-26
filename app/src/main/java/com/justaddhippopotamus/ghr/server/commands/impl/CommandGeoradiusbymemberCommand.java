
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeoradiusbymemberCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        generic(item);
    }

    public static void generic(WorkItem item) {
        //GEORADIUSBYMEMBER key member radius <M | KM | FT | MI> [WITHCOORD]
        //  [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC] [STORE key
        //  | STOREDIST key]
        var commands = item.scanner();
        var key = commands.key();
        var member = commands.key();
        double radius = commands.takeDouble();
        var units = commands.argOneOf("M","KM","FT","MI");
        boolean WITHCOORD = commands.argIs("WITHCOORD");
        boolean WITHDIST = commands.argIs("WITHDIST");
        boolean WITHHASH = commands.argIs("WITHHASH");
        boolean COUNT = commands.argIs("COUNT");
        int count = COUNT?commands.takeInt():0;
        boolean ANY = commands.argIs("ANY");
        boolean ASC = commands.argIs("ASC");
        boolean DESC = commands.argIs("DESC");
        if( !DESC ) ASC=true;
        var store = commands.argIs("STORE")?commands.key():null;
        var storeDist = commands.argIs("STOREDIST")?commands.key():null;
        var geo = item.getMainStorage().fetchRO(key, RedisGeo.class);
        if( geo == null ) {
            item.whoFor.queueNullArray(item.order);
        } else {
            Double hash = geo.getScore(member);
            if( hash == null ) {
                item.whoFor.queueNullArray(item.order);
            } else {
                double [] xy = {0,0};
                RedisGeo.degeohash(hash,xy);
                CommandGeoradiusCommand.generic(item,key,xy[RedisGeo.XYLONG],xy[RedisGeo.XYLAT],radius,units,WITHCOORD,WITHDIST,WITHHASH,count,ANY,ASC,DESC,store,storeDist);
            }
        }
    }
}
