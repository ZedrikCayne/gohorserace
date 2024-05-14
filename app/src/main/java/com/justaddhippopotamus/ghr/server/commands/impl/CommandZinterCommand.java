
package com.justaddhippopotamus.ghr.server.commands.impl;

import com.justaddhippopotamus.ghr.RESP.RESPArrayScanner;
import com.justaddhippopotamus.ghr.server.ICommandImplementation;
import com.justaddhippopotamus.ghr.server.Client;
import com.justaddhippopotamus.ghr.server.WorkItem;
import com.justaddhippopotamus.ghr.server.types.RedisSortedSet;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.util.*;

/* File just to have a class here for the ServerCommands to import should this
   directory be otherwise empty.
 */
public class CommandZinterCommand extends ICommandImplementation {
    public enum AGGREGATE_MODE  {
        AGGREGATE_MODE_SUM,
        AGGREGATE_MODE_MIN,
        AGGREGATE_MODE_MAX
    }

    private static class KWSET implements Comparable<KWSET> {
        String key;
        double weight;
        RedisSortedSet set;

        public KWSET(String key, double weight, RedisSortedSet set) {
            this.key = key; this.weight = weight; this.set = set;
        }

        @Override
        public int compareTo(KWSET o) {
            return Integer.compare(set.size(),o.set.size());
        }
    }
    public static RedisSortedSet genericZinter(List<String> keys, List<Double> weights, AGGREGATE_MODE mode, Client client, boolean unionMode) {
        List<RedisSortedSet> allSets = client.getMainStorage().fetchROMany(keys,RedisSortedSet.class);
        //On any empty set (null) there's no intersection...return empty.
        if( !unionMode ) {
            if (allSets.contains(null)) {
                return new RedisSortedSet();
            }
        }
        return RedisType.atomicAllStatic(allSets, RedisSortedSet.class, all -> {
            int len = keys.size();
            List<KWSET> kwsets = new ArrayList<>(len);
            for( int i = 0; i < len; ++i ) {
                kwsets.add(new KWSET(keys.get(i), weights != null ? weights.get(i) : 1.0d, all.get(i)));
            }
            if( !unionMode ) kwsets.sort(KWSET::compareTo);
            Map<String,Double> result = kwsets.get(0).set.toMap();
            Set<String> keysToNuke = new HashSet<>(result.size());
            for( int i = 1; i < len; ++i ) {
                keysToNuke.clear();
                KWSET kwset = kwsets.get(i);
                RedisSortedSet rs = kwset.set;
                for( String key : unionMode?rs.keys():result.keySet() ) {
                    Double score = rs.getScore(key);
                    if( score == null ) {
                        if( !unionMode )
                            keysToNuke.add(key);
                    } else {
                        switch(mode) {
                            case AGGREGATE_MODE_SUM:
                                result.put(key,result.getOrDefault(key,0.0D) + score);
                                break;
                            case AGGREGATE_MODE_MAX:
                                if( score > result.getOrDefault(key,Double.MIN_VALUE) )
                                    result.put(key,score);
                                break;
                            case AGGREGATE_MODE_MIN:
                                if( score < result.getOrDefault(key,Double.MAX_VALUE) )
                                    result.put(key,score);
                                break;
                        }
                    }
                }
                for( String key : keysToNuke ) {
                    result.remove(key);
                }
                if( result.size() == 0 )
                    break;
            }
            return new RedisSortedSet(result);
        });
    }


    public static CommandZinterCommand.AGGREGATE_MODE getAggregateMode(RESPArrayScanner commands) {
        if( commands.argIs("AGGREGATE") ) {
            switch ( commands.argOneOfRequired("MIN","MAX","SUM") ) {
                case "MIN":
                    return CommandZinterCommand.AGGREGATE_MODE.AGGREGATE_MODE_MIN;
                case "MAX":
                    return CommandZinterCommand.AGGREGATE_MODE.AGGREGATE_MODE_MAX;
                case "SUM":
                    break;
                default:
                    throw new RuntimeException("Bad args");
            }
        }
        return CommandZinterCommand.AGGREGATE_MODE.AGGREGATE_MODE_SUM;
    }

    public static void zInterUnionGenericCommandWithQueue(WorkItem item) {
        final RESPArrayScanner commands = item.scanner();
        final Client client = item.whoFor;
        final long order = item.order;
        String outputKey = null;
        boolean outputCardinal = false;
        boolean unionMode = false;
        int limit = 0;
        switch( commands.command() ) {
            case "ZINTERCARD":
                outputCardinal = true;
            case "ZINTER":
                break;
            case "ZINTERSTORE":
                outputKey = commands.key();
                break;
            case "ZUNIONSTORE":
                outputKey = commands.key();
            case "ZUNION":
                unionMode = true;
                break;
            default:
                throw new RuntimeException("Bad arguments for ZINTER*");
        }
        List<String> keys = commands.getNumKeys();
        int numKeys = keys.size();
        List<Double> weights = null;
        if( commands.argIs("WEIGHTS") ) {
            weights = commands.takeDoubles(numKeys);
        }
        AGGREGATE_MODE mode = getAggregateMode(commands);
        boolean WITHSCORES = commands.argIs("WITHSCORES");
        if( commands.argIs("LIMIT") ) limit = commands.takeInt();

        RedisSortedSet rss = genericZinter(keys,weights,mode,client,unionMode);

        if( outputKey != null ) {
            client.getMainStorage().store(outputKey, rss);
            client.queueInteger(rss.size(),order);
        } else {
            if( outputCardinal ) {
                int rssSize = rss.size();
                client.queueInteger(limit!=0&&rssSize>limit?limit:rssSize,order);
            } else {
                client.queue(rss.toArray(WITHSCORES),order);
            }

        }
    }
    @Override
    public void runCommand(WorkItem item) {
        zInterUnionGenericCommandWithQueue(item);
    }
}
