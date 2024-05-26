
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Command;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisGeo;

import java.util.ArrayList;
import java.util.List;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandGeoaddCommand extends ICommandImplementation {
    @Override
    public void runCommand(WorkItem item) {
        //GEOADD key [NX | XX] [CH] longitude latitude member [longitude
        //  latitude member ...]
        var commands = item.scanner();
        var key = commands.key();
        boolean NX = commands.argIs("NX");
        boolean XX = commands.argIs("XX");
        boolean CH = commands.argIs("CH");
        List<RedisGeo.GeoItem> items = new ArrayList<>();
        while( commands.hasNext() ) {
            RedisGeo.GeoItem gi = new RedisGeo.GeoItem();
            gi.longitude = commands.takeDouble();
            gi.latitude = commands.takeDouble();
            gi.name = commands.string();
            items.add(gi);
        }
        RedisGeo rg = item.getMainStorage().fetchRW(key,RedisGeo.class,RedisGeo::new);
        item.whoFor.queueInteger(rg.addGeo(items,NX,XX,CH),item.order);
    }
}
