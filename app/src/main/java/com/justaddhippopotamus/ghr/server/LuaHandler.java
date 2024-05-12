package com.justaddhippopotamus.ghr.server;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.justaddhippopotamus.ghr.RESP.*;
import com.justaddhippopotamus.ghr.server.types.RedisType;
import org.luaj.vm2.*;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.*;
import org.luaj.vm2.lib.jse.*;

public class LuaHandler {
    public static class LuaClient extends Client {
        @Override
        public void queue(IRESP sendOverTheWire,long order) {
            try {
                myQueue.put(sendOverTheWire);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        public BlockingQueue<IRESP> myQueue = new LinkedBlockingQueue<>();

        LuaClient(Socket clientSocket, Server server, int id, boolean authed) {
            super(clientSocket, server, id, authed);
        }
    }

    public static LuaValue respToLua(IRESP object) {
        if( object instanceof RESPInteger ) {
            RESPInteger integer = (RESPInteger)object;
            return LuaValue.valueOf(integer.value);
        }
        if( object instanceof RESPBulkString ) {
            RESPBulkString bulkString = (RESPBulkString)object;
            //Special case, is it null?
            if( bulkString.value == null ) return LuaValue.valueOf(false);
            return LuaValue.valueOf(bulkString.value);
        }
        if( object instanceof RESPSimpleString ) {
            RESPSimpleString simpleString = (RESPSimpleString)object;
            LuaTable lt = new LuaTable();
            lt.set("ok",simpleString.toString());
            return lt;
        }
        if( object instanceof RESPSimpleError ) {
            RESPSimpleError err = (RESPSimpleError)object;
            LuaTable lt = new LuaTable();
            lt.set("err", err.toString());
            return lt;
        }
        if( object instanceof RESPArray ) {
            RESPArray array = (RESPArray)object;
            LuaTable lt = LuaTable.tableOf();
            for( int i = 0; i < array.size(); ++i ) {
                lt.set(i+1, respToLua(array.value.get(i)));
            }
            return lt;
        }
        return LuaValue.valueOf(false);
    }

    public static IRESP luaToResp(LuaValue object) {
        if( object.isnumber() ) {
            return new RESPInteger(object.checkint());
        } else if( object.isstring() ) {
            return new RESPBulkString(object.checkstring().m_bytes);
        } else if( object.istable() ) {
            LuaTable t = (LuaTable)object;
            LuaValue v = t.get("ok");
            if( v != null && !v.isnil() ) {
                return new RESPSimpleString(v.toString());
            } else {
                v = t.get("err");
                if( v != null && !v.isnil() ) {
                    return new RESPSimpleError(v.toString());
                } else {
                    RESPArray ra = new RESPArray();
                    LuaValue k = LuaValue.NIL;
                    for( int i = 0; true; ++i ) {
                        Varargs n = t.next(k);
                        k = n.arg1();
                        if( k.isnil() )
                            break;
                        ra.addRespElement(luaToResp(k));
                    }
                    return ra;
                }
            }
        } else if( object.isboolean() ) {
            LuaBoolean b = (LuaBoolean)object;
            if(!b.booleanValue()) {
                return Client.NIL_BULK_STRING;
            }
        } else if( object.isnil()) {
            return Client.NIL_BULK_STRING;
        }
        return null;
    }

    public static class redis extends TwoArgFunction {
        public static class call extends VarArgFunction {
            LuaClient luaClient;
            public call(LuaClient lc) {this.luaClient = lc;}
            public Varargs invoke(Varargs args) {
                int nargs = args.narg();
                RESPArray argumentsForRedis = new RESPArray();
                for( int i = 1; i <= nargs; ++i ) {
                    LuaValue v = args.arg(i);
                    if( v.isstring() ) argumentsForRedis.addRespElement( new RESPBulkString(v.checkstring().m_bytes));
                    else argumentsForRedis.addRespElement(new RESPBulkString(String.valueOf(args.arg(i))));
                }
                if(Server.verbose)
                    System.out.println("LUA>: " + argumentsForRedis.prettyString() );
                try{
                    luaClient.myServer.executeBlocking( argumentsForRedis, luaClient, 0);
                } catch (Exception e) {
                    luaClient.myServer.executeBlocking( argumentsForRedis, luaClient, 0);
                }
                IRESP response = null;
                try {
                    response = luaClient.myQueue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if( response instanceof RESPSimpleError) {
                    luaClient.myServer.executeBlocking( argumentsForRedis, luaClient, 0);
                }

                if( response == null ) {
                    if(Server.verbose)
                        System.out.println("LUA<: null");
                    return NONE;

                }
                if(Server.verbose)
                    System.out.println("LUA<: " + response.prettyString() );
                //Response is an IRESP...
                return respToLua(response);
            }
        }

        public LuaValue call(LuaValue modname, LuaValue env) {
            LuaTable table = new LuaTable();
            myCall = new redis.call(luaClient);
            table.set("call", myCall);
            env.set("redis", table);
            env.get("package").get("loaded").set("redis", table);
            return NIL;
        }
        private call myCall;
        LuaClient luaClient;

        public redis(LuaClient luaClient) { this.luaClient = luaClient; }
    }
    Globals globals;
    redis myRedis;
    LuaClient luaClient;
    Client client;

    private Globals boxedGlobals() {
        Globals g = new Globals();
        g.load(new JseBaseLib());
        g.load(new PackageLib());
        g.load(new Bit32Lib());
        g.load(new TableLib());
        g.load(new StringLib());
        g.load(new JseMathLib());
        LoadState.install(g);
        LuaC.install(g);
        return g;
    }
    public LuaHandler( Client whoFor ) {
        globals = boxedGlobals();
        //globals = JsePlatform.standardGlobals();
        luaClient = new LuaClient(null, whoFor.myServer, -1, whoFor.isAuthed());
        myRedis = new redis(luaClient);
        globals.load(myRedis);
        client = whoFor;
    }

    private void setGlobal(String key, List<String> keys) {
        if( keys == null ) {
            globals.set(key,LuaValue.NIL);
        } else {
            int len = keys.size();
            LuaTable t = new LuaTable();
            for (int i = 0; i < len; ++i) {
                t.set(i+1, keys.get(i));
            }
            globals.set(key, t);
        }
    }
    private void setGlobalBulk(String key, List<RESPBulkString> keys) {
        if (keys == null) {
            globals.set(key, LuaValue.NIL);
        } else {
            int len = keys.size();
            LuaTable t = new LuaTable();
            for (int i = 0; i < len; ++i) {
                //Let's peer at our values a moment...
                RESPBulkString which = keys.get(i);
                t.set(i+1, LuaString.valueOf(which.value));
            }
            globals.set(key, t);
        }
    }

    public IRESP eval(Client client, String what, List<String> keys, List<RESPBulkString> args) {
        return RedisType.atomicAllStatic(client.getMainStorage().mget(keys), IRESP.class, all -> {
            LuaValue script = globals.load(what);
            setGlobal("KEYS",keys);
            setGlobalBulk("ARGV",args);
            StringBuilder sb = new StringBuilder();
            sb.append("LuaCall: Keys: ");
            keys.stream().forEach(s -> {sb.append(s);sb.append(",");});
            sb.deleteCharAt(sb.length()-1);
            if( args != null ) {
                sb.append(" Args: ");
                args.stream().forEach(s -> { sb.append(s.prettyString()); sb.append(","); });
                sb.deleteCharAt(sb.length()-1);
            }

            if(Server.verbose) System.out.println(sb.toString());
            Varargs output = script.call();

            //Script
            return luaToResp(output.arg1());
        });
    }
}
//local tasksSet=KEYS[1]              -- Hash set of all tasks, indexed by task ID\nlocal taskTimeoutsSet=KEYS[2]       -- Hash set of task timeouts, indexed by task ID\nlocal pendingTaskIdsList=KEYS[3]    -- List of pending task IDs\nlocal activeTaskIdsSet=KEYS[4]      -- Sorted set of active task IDs, indexed by timeout\nlocal maxTasks=tonumber(ARGV[1])    -- Max tasks to accept\nlocal currentTime=tonumber(ARGV[2]) -- Current epoch time in milliseconds\n\nlocal acceptedTasks = {}\nwhile #acceptedTasks < maxTasks\ndo\n    local taskId = nil\n    local taskEntries = redis.call(\"zrangebyscore\", pendingTaskIdsList, \"-inf\", currentTime, \"limit\", \"0\", \"1\")\n    if #taskEntries > 0 then\n        taskId = taskEntries[1]\n        redis.call(\"zrem\", pendingTaskIdsList, taskId)\n    else\n        local taskIds = redis.call(\"zrangebyscore\", activeTaskIdsSet, \"-inf\", currentTime, \"limit\", \"0\", \"1\")\n        if #taskIds > 0 then\n            taskId = taskIds[1]\n        end\n    end\n\n    if taskId then\n        local task = redis.call(\"hget\", tasksSet, taskId)\n        local timeout = tonumber(redis.call(\"hget\", taskTimeoutsSet, taskId))\n        redis.call(\"zadd\", activeTaskIdsSet, tostring(currentTime + timeout), taskId)\n        table.insert(acceptedTasks, task)\n    else\n        break\n    end\nend\n\nreturn acceptedTasks\n