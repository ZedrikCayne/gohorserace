
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.HashMap;
import java.util.Map;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeoradiusCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        generic(item);
    }

    public static void generic(WorkItem item, String key, double longitude, double latitude, double radius,
                        String units, boolean WITHCOORD, boolean WITHDIST, boolean WITHHASH,
                        int count, boolean ANY, boolean ASC, boolean DESC, String store, String storeDist ) {
        var geo = item.getMainStorage().fetchRO(key, RedisGeo.class);
        radius = RedisGeo.distanceToMeters(radius,units);
        if( geo == null ) {
            item.whoFor.queueNullArray(item.order);
        } else {
            var elements = geo.find(longitude,latitude,radius);
            while( ANY && elements.size() > count ) elements.remove(elements.size()-1);
            if( ASC ) elements.sort(RedisGeo.SetDistancePosition::compareTo);
            if( DESC ) elements.sort(RedisGeo.SetDistancePosition::inverseCompareTo);
            while( count > 0 && elements.size() > count ) elements.remove( elements.size() - 1);
            if( store != null || storeDist != null ) {
                Map<String,Double> toAdd = new HashMap<>(elements.size());
                RedisSortedSet rss;
                if( store != null ) {
                    elements.forEach( e -> toAdd.put(e.key,(double)e.hash) );
                    rss = new RedisSortedSet();
                } else {
                    elements.forEach( e -> toAdd.put(e.key,e.distance) );
                    rss = new RedisGeo();
                }
                item.whoFor.queueInteger(rss.add(toAdd,false,false,false,false,false),item.order);
                item.getMainStorage().store(store==null?storeDist:store,rss);
            } else {
                RESPArray returnValue = new RESPArray(elements.size());
                if (!WITHCOORD && !WITHDIST && !WITHHASH) {
                    for (var e : elements) returnValue.addString(e.key);
                } else {
                    for (var e : elements) {
                        RESPArray r = new RESPArray(4);
                        r.addString(e.key);
                        if (WITHDIST)
                            r.addString(Utils.doubleToStringRedisGeoStyle(RedisGeo.convertDistance(e.distance, units)));
                        if (WITHHASH) r.addString(String.valueOf(e.hash));
                        if (WITHCOORD) {
                            RESPArray coords = new RESPArray(2);
                            coords.addString(Utils.doubleToStringRedisStyle(e.longitude));
                            coords.addString(Utils.doubleToStringRedisStyle(e.latitude));
                            r.addRespElement(coords);
                        }
                        returnValue.addRespElement(r);
                    }
                }
                item.whoFor.queue(returnValue, item.order);
            }
        }
    }

    public static void generic(WorkItem item) {
        //GEORADIUS key longitude latitude radius <M | KM | FT | MI>
        //  [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
        //  [STORE key | STOREDIST key]
        var commands = item.scanner();
        var key = commands.key();
        double longitude = commands.takeDouble();
        double latitude = commands.takeDouble();
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
        generic(item,key,longitude,latitude,radius,units,WITHCOORD,WITHDIST,WITHHASH,count,ANY,ASC,DESC,store,storeDist);
    }
}
