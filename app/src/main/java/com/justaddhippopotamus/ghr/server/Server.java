package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.*;
import com.justaddhippopotamus.ghr.server.commands.ServerCommands;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends GoDog {
    private static class ServerExecutor extends GoDog {
        private Server myServer;
        private boolean idling = false;
        public ServerExecutor(Server server) {
            myServer = server;
        }


        public boolean isBusy() { return !idling; }

        @Override
        public void run() {
            super.run();
            while(running) {
                try {
                    idling = true;
                    WorkItem workItem = myServer.workItemQueue.take();
                    if( workItem.alreadyCompleted() )
                        continue;
                    if(Server.verbose)
                        System.out.println(workItem.toString());
                    idling = false;
                    myServer.worker_execute(workItem);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static class ExpiryWorker extends GoDog {


    }
    public String dbFile = null;
    public int port = 0;
    public String password = null;

    public boolean hasPassword() {
        return password != null;
    }
    public int startingExecutors = 5;

    public boolean passwordValid(String username, String password) {

        //Maybe we do acl's later?
        if( password != null &&
            (this.password == null ||
            password.compareTo(this.password) == 0 )) {
            return true;
        }
        return password==null && this.password == null;
    }

    public Server(String file, int port, String password) {
        this.dbFile = file;
        this.port = port;
        this.password = password;
    }

    BlockingQueue<WorkItem> workItemQueue = new LinkedBlockingQueue<>();
    Map<String,Command> commandTable = ServerCommands.redisCommandTable;
    List<ServerExecutor> serverExecutors = new ArrayList<>();
    Map<String, Command> commandLookup;

    private boolean runCommandTrueOnNoAuth(Command command, WorkItem item) {
        if( item.whoFor.isAuthed() || command.flags.contains(Command.CommandFlags.CMD_NO_AUTH) ) {
            command.implementation.runCommand(item);
            return false;
        }
        return true;
    }

    private Command getCommandWithImpl(WorkItem item) {
        RESPArrayScanner commands = item.scanner();
        Command c = commandTable.getOrDefault(commands.command(),null);
        if( c != null && c.implementation == null ) {
            c = c.subcommands.getOrDefault(commands.subcommand(),null);
            if( c != null && c.implementation == null ) {
                return null;
            }
        }
        return c;
    }
    public void worker_execute(WorkItem item) {
        if( item.what == null ) return;
        Command c = getCommandWithImpl(item);
        if( c == null ) {
            item.whoFor.queueSimpleError("I don't understand " + item.toString(), item.order);
        } else {
            try  {
                if( runCommandTrueOnNoAuth(c,item) ) {
                    item.whoFor.queueSimpleError("Need to AUTH first.", item.order );
                }
            } catch (RuntimeException e) {
                String message = e.getMessage();
                if( message == null ) {
                    e.printStackTrace();
                    message = c.declared_name + " failed";
                }
                item.whoFor.queueSimpleError(message, item.order);
            }
        }
    }
    public void executeMany(List<RESPArray> items, Client client, long order) {
        client.startExecute();
        for( RESPArray item : items ) {
            worker_execute(new WorkItem(item, client, order));
        }
        client.execute(order);
    }
    public void execute(RESPArray parameters, Client client, long order) {
        try {
            workItemQueue.put(new WorkItem(parameters, client, order));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void execute(WorkItem i) {
        try {
            workItemQueue.put(i);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void executeBlocking(RESPArray parameters, Client client, long order) {
        worker_execute(new WorkItem(parameters,client,order));
    }
    @Override
    public void run() {
        super.run();
        ServerCommands.init("/commands/");

        //Start up storage.
        mainStorage = new TypeStorage(dbFile,1000);
        luaStorage = new LuaStorage();
        channels = new ChannelManager();

        //Main listener
        mainListener = new Listener(port,this);
        mainListener.goDogGo();

        //Server Executors. (Grab stuff from the work queue)
        for(int i = 0; i < startingExecutors; ++i ) {
            ServerExecutor e = new ServerExecutor(this);
            e.goDogGo();
            serverExecutors.add(e);
        }

        while (running) {
            try {
                Thread.sleep(1000);
                int count = 0;
                for( ServerExecutor s : serverExecutors ) {
                    if(s.isBusy())++count;
                }
                //System.out.println(count + " busy");
                //System.out.println(workItemQueue.size() + " queue depth");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        mainLoopCompleted();
    }
    @Override
    public boolean stop() {
        boolean wasRunning = super.stop();
        if( wasRunning ) {
            mainListener.stop();
            for (var worker : clients) {
                worker.stop();
            }
            for (var executor : serverExecutors ){
                executor.stop();
                //Offer a null work item for every executor to ensure threads
                //stop on an idle server
                try {
                    workItemQueue.put(new WorkItem());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            mainStorage.shutDown();
        }
        return wasRunning;
    }

    public void addClient(Client client) {
        clients.add(client);
    }
    public TypeStorage mainStorage = null;
    public LuaStorage luaStorage = null;
    public ChannelManager channels = null;
    public Listener mainListener = null;
    public List<Client> clients = new ArrayList<>(5);

    public static final Charset CHARSET = StandardCharsets.UTF_8;

    public LuaStorage luaStorage() { return luaStorage; }
    public ChannelManager channelManager() { return channels; }
    public TypeStorage getStorage(int db) {
        return mainStorage.getStorage(db);
    }

    public static volatile boolean verbose = false;
}
