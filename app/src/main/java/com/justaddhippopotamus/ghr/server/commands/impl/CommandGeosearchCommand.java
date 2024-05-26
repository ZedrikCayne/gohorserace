
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArray;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.Utils;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;

import java.util.HashMap;
import java.util.Map;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeosearchCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //GEOSEARCH key <FROMMEMBER member | FROMLONLAT longitude latitude>
        //  <BYRADIUS radius <M | KM | FT | MI> | BYBOX width height <M | KM |
        //  FT | MI>> [ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST]
        //  [WITHHASH]
        generic(item);
    }

    public static void generic(WorkItem item) {
        //GEOSEARCH key <FROMMEMBER member | FROMLONLAT longitude latitude>
        //  <BYRADIUS radius <M | KM | FT | MI> | BYBOX width height <M | KM |
        //  FT | MI>> [ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST]
        //  [WITHHASH]
        var commands = item.scanner();
        var store = commands.commandIs("GEOSEARCHSTORE")?commands.key():null;
        var key = commands.key();
        boolean FROMMEMBER = commands.argIsRequired("FROMLONLAT", "FROMMEMBER" );
        var geo = item.getMainStorage().fetchRO(key, RedisGeo.class);
        if( geo == null ) {
            if( store != null ) {
                item.whoFor.queueInteger(0,item.order);
            } else {
                item.whoFor.queueEmptyArray(item.order);
            }
            return;
        }
        double [] xy = {0,0};
        if( FROMMEMBER ) {
            var member = commands.key();
            Double hash = geo.getScore(member);
            if( hash == null ) {
                item.whoFor.queueEmptyArray(item.order);
                return;
            }
            RedisGeo.degeohash(hash,xy);
        }
        double longitude = FROMMEMBER?xy[RedisGeo.XYLONG]:commands.takeDouble();
        double latitude = FROMMEMBER?xy[RedisGeo.XYLAT]:commands.takeDouble();
        boolean BYBOX = commands.argIsRequired("BYRADIUS","BYBOX");
        double width = BYBOX?commands.takeDouble():0;
        double height = BYBOX?commands.takeDouble():0;
        double radius = BYBOX?0:commands.takeDouble();
        String units = commands.argOneOf("M", "KM", "FT", "MI" );
        width = RedisGeo.distanceToMeters(width,units);
        height = RedisGeo.distanceToMeters(width,units);
        radius = RedisGeo.distanceToMeters(radius,units);
        boolean ASC = commands.argIs("ASC");
        boolean DESC = commands.argIs( "DESC");
        boolean COUNT = commands.argIs("COUNT");
        int count = COUNT?commands.takeInt():0;
        boolean ANY = commands.argIs("ANY");
        boolean WITHCOORD = commands.argIs("WITHCOORD");
        boolean WITHDIST = commands.argIs("WITHDIST");
        boolean WITHHASH = commands.argIs("WITHHASH");
        boolean STOREDIST = commands.argIs("STOREDIST");

        var elements = BYBOX?geo.findBox(longitude,latitude,width,height):geo.find(longitude,latitude,radius);
        while( ANY && elements.size() > count ) elements.remove(elements.size()-1);
        if( ASC ) elements.sort(RedisGeo.SetDistancePosition::compareTo);
        if( DESC ) elements.sort(RedisGeo.SetDistancePosition::inverseCompareTo);
        while( count > 0 && elements.size() > count ) elements.remove( elements.size() - 1);
        if( store != null ) {
            Map<String,Double> toAdd = new HashMap<>(elements.size());
            RedisSortedSet rss;
            if( !STOREDIST ) {
                elements.forEach( e -> toAdd.put(e.key,(double)e.hash) );
                rss = new RedisSortedSet();
            } else {
                elements.forEach( e -> toAdd.put(e.key,e.distance) );
                rss = new RedisGeo();
            }
            item.whoFor.queueInteger(rss.add(toAdd,false,false,false,false,false),item.order);
            item.getMainStorage().store(store,rss);
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
