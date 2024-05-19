package com.justaddhippopotamus.ghr.server;

import com.justaddhippopotamus.ghr.RESP.*;
import com.justaddhippopotamus.ghr.server.types.RedisString;
import com.justaddhippopotamus.ghr.server.types.RedisType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class Client extends GoDog {
    private static class StuffToSend implements Comparable<StuffToSend> {
        public IRESP theData;
        public long order;

        public StuffToSend(IRESP theData, long order ) {
            this.theData = theData;
            this.order = order;
        }

        @Override
        public int compareTo(StuffToSend o) {
            return Long.compare(order,o.order);
        }
    }
    private static class ClientWriter extends GoDog {
        public ClientWriter( Client myClient ) {
            this.myClient = myClient;
        }
        long orderNext = 1;
        private boolean resetting = false;
        @Override
        public void run() {
            super.run();
            try {
                myStream = myClient.mySocket.getOutputStream();
            } catch (IOException e) {
                running = false;
            }
            while( running && myStream != null && !myClient.stopRequested ) {
                try {
                    if( resetting ) {
                        List<StuffToSend> drainBoard = new ArrayList<>();
                        myClient.stuffToSend.drainTo(drainBoard);
                        drainBoard = drainBoard.stream().filter(x -> x.order == 1).collect(Collectors.toList());
                        if( !drainBoard.isEmpty() ) {
                            try {
                                resetting = false;
                                myClient.stuffToSend.put(drainBoard.get(0));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    StuffToSend nextToSend = myClient.stuffToSend.take();
                    if (!resetting && (nextToSend.order == 0 || nextToSend.order == orderNext) ) {
                        if( Server.verbose && nextToSend.theData != NOP)
                            LOG.trace(myClient.hashCode() + ": reply " + nextToSend.theData.prettyString());
                        nextToSend.theData.publishTo(myStream);
                        if (nextToSend.order == orderNext)
                            ++orderNext;
                    } else {
                        //While we're resetting...eat everything but #1
                        if( !resetting || nextToSend.order == 1 )
                            myClient.stuffToSend.add(nextToSend);
                    }
                }catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    stopIfNecessary();
                }
            }
            mainLoopCompleted();
        }

        public void reset() {
            resetting = true;
            orderNext = 1;
        }

        public boolean stop() {
            boolean wasRunning = super.stop();
            if( wasRunning ) {
                try {
                    if (myStream != null)
                        myStream.close();
                    myStream = null;
                } catch (IOException e) {
                }
                //Enqueue a nop to make sure our threads finish up.
                myClient.queueHardNop();
            }
            return wasRunning;
        }

        public Client myClient;
        public OutputStream myStream;
    }
    int id;
    public List<RESPArray> queuedStuff = new LinkedList<>();
    public ConcurrentHashMap<WorkItem,Long> blocking = new ConcurrentHashMap<>();
    public Client(TypeStorage s) {
        authed = false;
        mySocket = null;
        myServer = null;
        id = -1;
        mainStorage = s;
    }
    public Client(Socket clientSocket, Server server, int id, boolean authed, int storage) {
        mySocket = clientSocket;
        myServer = server;
        this.id = id;
        this.authed = authed;
        select(storage);
    }
    @Override
    public void run() {
        super.run();
        IRESPFactory factory = IRESPFactory.getDefault();
        myWriter = new ClientWriter(this);
        myWriter.goDogGo();
        try{
            myStream = mySocket.getInputStream();
        } catch (Exception e) {
            stopIfNecessary();
        }

        while(running && myStream != null) {
            try {
                int nextByte = myStream.read();
                RESPArray rarray = null;
                if( nextByte < 0 ) {
                    LOG.trace(hashCode() + " socket closed.");
                    stop();
                } else if ((char)nextByte == '*' ) {
                    //According to the docs, all commands are RESP Arrays.
                    rarray = new RESPArray(factory, myStream);
                } else {
                    //Or we are in run me by telnet mode...
                    rarray = new RESPArray();
                    StringBuilder sb = new StringBuilder();
                    do {
                        if( !whiteSpace(nextByte) ) {
                            sb.append((char) nextByte);
                        } else {
                            String commandOrArgument = sb.toString();
                            rarray.addRespElement(new RESPBulkString(commandOrArgument));
                            sb = new StringBuilder();
                        }
                        nextByte = myStream.read();
                    } while( myStream.available() > 0 && !isCrLF(nextByte));
                    if( sb.length() > 0 )
                        rarray.addRespElement(new RESPBulkString(sb.toString()));
                    //Eat any remaining trash (just in case)
                    while( myStream.available() > 0 )
                        myStream.read();
                }


                if( rarray != null ) {
                    if( Server.verbose ) {
                        LOG.info(this.hashCode() + ": "+ rarray.prettyString());
                    }
                    RESPArrayScanner commands = new RESPArrayScanner(rarray);
                    if( multiMode && !commands.commandIs("EXEC", "DISCARD", "RESET" ) ) {
                        queuedStuff.add(rarray);
                        queue(QUEUED,order);
                    }
                    else
                    {
                        myServer.execute(rarray,this, order);
                    }
                    ++order;
                }
            } catch( IOException e ){
                stopIfNecessary();
            }
        }
        mainLoopCompleted();
    }

    private boolean whiteSpace(int nextByte) throws IOException {
        switch( nextByte ) {
            case -1:
                throw new IOException("EOF");
            case IRESP.CR:
            case IRESP.LF:
            case IRESP.SPACE:
                return true;
            default:
                return false;
        }
    }

    private boolean isCrLF( int nextByte ) throws IOException {
        switch( nextByte ) {
            case -1:
                throw new IOException("EOF");
            case IRESP.CR:
            case IRESP.LF:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean stop() {
        boolean wasRunning = super.stop();
        if( wasRunning ) {
            try {
                if (myWriter != null ) {
                    myWriter.stop();
                    myWriter = null;
                }
                if (mySocket != null) {
                    mySocket.close();
                }
                for (PubSubChannel channel: channelsISubscribeTo.values()) {
                    channel.unsubscribe(this);
                }
            } catch (Exception e) {
            }
            mySocket = null;
        }
        return wasRunning;
    }

    private static final RESPBulkString PSUBSCRIBE = new RESPBulkString("psubscribe");
    private static final RESPBulkString PUNSUBSCRIBE = new RESPBulkString("punsubscribe");
    private static final RESPBulkString SUBSCRIBE = new RESPBulkString("subscribe");
    private static final RESPBulkString UNSUBSCRIBE = new RESPBulkString("unsubscribe");
    private static final RESPBulkString MESSAGE = new RESPBulkString("message");
    public boolean queuePubSubMessage(RESPBulkString channel, RESPBulkString message) {
        if( !running ) return true;
        RESPPush respPush = new RESPPush(clientRESPVersion== IRESP.RESPVersion.RESP3,MESSAGE,channel,message);
        queue(respPush,0);
        return false;
    }

    public void queuePubSubSubscribe(RESPBulkString channel,boolean psub) {
        RESPPush respPush = new RESPPush(clientRESPVersion== IRESP.RESPVersion.RESP3,
                psub?PSUBSCRIBE:SUBSCRIBE,channel,new RESPInteger(psub?patternsISubscribeTo.size():channelsISubscribeTo.size()));
        queue(respPush,0);
    }

    public void queuePubSubUnsubscribe(RESPBulkString channel,boolean psub) {
        RESPPush respPush = new RESPPush(clientRESPVersion== IRESP.RESPVersion.RESP3,
                psub?PUNSUBSCRIBE:UNSUBSCRIBE,channel,new RESPInteger(psub?patternsISubscribeTo.size():channelsISubscribeTo.size()));
        queue(respPush,0);
    }

    public void subscribeTo( PubSubChannel psc ) {
        if( psc.getName().compareTo("RESET") == 0 ) {
            myServer.execute(RESET_COMMAND,this,0);
        } else {
            channelsISubscribeTo.put(psc.getName(), psc);
            psc.subscribe(this);
            queuePubSubSubscribe(psc.getBulkname(),false);
        }
    }

    public void reset() {
        //Kill all outgoing.
        myWriter.reset();
        //Ditch everything. This stuff will try to queue but the order numbers
        //will all be higher than 1.
        select(0);
        unsubscribeAll();
        unsubscribeAllPatterns();
        discardMulti();
        clientRESPVersion = IRESP.RESPVersion.RESP2;
        if( myServer.hasPassword() ) authed = false;
        //And now, reset our order...so the next thing that comes through (Which should be RESET which we hard code to 1.
        //And the next thing should be 2.
        order = 2;
    }

    public void unsubscribeTo( PubSubChannel psc ) {
        channelsISubscribeTo.remove(psc.getName());
        psc.unsubscribe(this);
        queuePubSubUnsubscribe(psc.getBulkname(),false);
    }

    RESPArray RESET_COMMAND = new RESPArray("RESET");

    public void subscribeToPattern(PatternHolder ph) {
        if( ph.pattern.compareTo("RESET") == 0 ) {
            myServer.execute(RESET_COMMAND,this,0);
        } else {
            patternsISubscribeTo.put(ph.pattern, ph);
            ph.addClient(this);
        }
    }

    public void unsubscribeToPattern(PatternHolder ph ) {
        patternsISubscribeTo.remove(ph.pattern);
        ph.removeClient(this);
    }

    public void unsubscribeAll() {
        Set<PubSubChannel> channelsToRemove = new HashSet<>(channelsISubscribeTo.values());
        channelsToRemove.stream().forEach(psc->unsubscribeTo(psc));
    }

    public void unsubscribeAllPatterns() {
        Set<PatternHolder> patternsToRemove = new HashSet<>(patternsISubscribeTo.values());
        patternsToRemove.forEach(ph-> unsubscribeToPattern(ph));
    }
    boolean multiMode = false;
    boolean multiExecuting = false;
    boolean multiModeError = false;
    public RESPArray multiReply = new RESPArray();

    public boolean setMulti() {
        boolean previous = multiMode;
        multiMode = true;
        multiModeError = false;
        multiExecuting = false;
        return previous;
    }


    private boolean stopRequested = false;

    public boolean discardMulti() {
        boolean previous = multiMode;
        multiMode = false;
        multiExecuting = false;
        multiModeError = false;
        queuedStuff = new LinkedList<>();
        return previous;
    }

    public boolean isMulti() {
        return multiMode;
    }

    public boolean startExecute() {
        boolean previous = multiExecuting;
        multiExecuting = true;
        return  previous;
    }
    public boolean execute(long order) {
        boolean previous = multiMode;
        multiMode = false;
        multiExecuting = false;
        queuedStuff = new LinkedList<>();
        queue(multiReply,order);
        multiReply = new RESPArray();
        return previous;
    }

    public boolean cancelTansaction(long order) {
        boolean previous = multiMode;
        multiMode = false;
        multiExecuting = false;
        return previous;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    Map<String,PubSubChannel> channelsISubscribeTo = new ConcurrentHashMap<>();
    Map<String, PatternHolder> patternsISubscribeTo = new ConcurrentHashMap<>();

    public void queue(IRESP sendOverTheWire,long order) {
        try {
            if( multiExecuting && order != 0 ) {
                multiReply.addRespElement(sendOverTheWire);
            } else {
                stuffToSend.put(new StuffToSend(sendOverTheWire, order));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void queue(Map<String,IRESP> map,long order) {
        if( clientRESPVersion == IRESP.RESPVersion.RESP2 ) {
            queue(new RESPArray(map),order);
        } else {
            queue(new RESPMap(map),order);
        }
    }

    public Map<String,IRESP> helloResponse() {
        Map<String,IRESP> values = new LinkedHashMap<>();
        values.put("server",new RESPBulkString("redis"));
        values.put("version",new RESPBulkString("7.2.0"));
        values.put("proto",new RESPInteger(clientRESPVersion== IRESP.RESPVersion.RESP2?2:3));
        values.put("id",new RESPInteger(id));
        values.put("mode",new RESPBulkString("standalone"));
        values.put("role",new RESPBulkString("master"));
        values.put("modules",new RESPArray());
        return values;
    }

    public void select(int storage) {
        mainStorage = myServer.getStorage(storage);
    }

    public void queueSimpleString(String simpleString, long order) {
        queue(new RESPSimpleString(simpleString),order);
    }

    public void queueBulkString(String bulkString,long order) {
        queue(new RESPBulkString(bulkString),order);
    }

    public void queueRedisStrings(Collection<RedisString> toSend, long order) {
        RESPArray respToSend = RESPArray.RESPArrayRedisStrings(toSend,clientRESPVersion);
        queue(respToSend, order);
    }

    public static IRESP getWireType(RedisType rt, IRESP.RESPVersion version) {
        if( rt == null ) {
            if( version == IRESP.RESPVersion.RESP3 )
                return Client.NULL;
            return Client.NIL_BULK_STRING;
        }
        return rt.wireType(version);
    }

    public void queue(RedisType sendOverTheWire, long order) {
        if( sendOverTheWire == null ) {
            queueNullBulkString(order);
        } else {
            queue(sendOverTheWire.wireType(clientRESPVersion), order);
        }
    }
    private long order = 1;
    protected Server myServer;
    protected TypeStorage mainStorage;
    private Socket mySocket;
    private boolean authed = false;
    public boolean isAuthed() { return authed; }
    public void auth() { authed = true; }
    private InputStream myStream = null;
    private ClientWriter myWriter = null;
    public IRESP.RESPVersion clientRESPVersion = IRESP.RESPVersion.RESP2;
    final BlockingQueue<StuffToSend> stuffToSend = new LinkedBlockingQueue<>();
    public static final RESPSimpleString OK = new RESPSimpleString("OK");
    public static final RESPSimpleString QUEUED = new RESPSimpleString("QUEUED");
    public static final RESPBulkString NIL_BULK_STRING = new RESPBulkString((byte[])null);
    public static final RESPArray NIL_ARRAY = new RESPArray((List<IRESP>)null);
    public static final RESPNull NULL = new RESPNull();
    public static final RESPArray EMPTY_ARRAY = new RESPArray();

    private final Map<Long,RedisCursor> cursors = new HashMap<>();


    public long getNewCursorId() {
        Random r = new Random();
        long rv = 0;
        while (rv <= 0 || cursors.containsKey(rv)) rv = r.nextLong();
        cursors.put(rv, null);
        return rv;
    }

    public void requestStop() {
        stopRequested = true;
    }

    public void populateCursor(RedisCursor c) {
        cursors.put(c.cursorId,c);
    }

    public void killCursor(long id) {
        cursors.remove(id);
    }

    public RedisCursor getCursor(long id) {
        if( cursors.containsKey(id) ) {
            return cursors.get(id);
        }
        return null;
    }



    public static final RESPNOP NOP = new RESPNOP();
    public void queueHardNop() { queue(NOP,0); }
    public void queueNop(long order) { queue(NOP,order); }
    public void queueOK(long order) { queue(OK,order); }
    public void queueNullBulkString(long order) { queue(clientRESPVersion == IRESP.RESPVersion.RESP2 ? NIL_BULK_STRING : NULL, order); }
    public void queueNullArray(long order) { queue(clientRESPVersion == IRESP.RESPVersion.RESP2 ? NIL_ARRAY : NULL, order); }
    public void queueSimpleError(String error, long order) { queue( new RESPSimpleError(error), order); }
    public void queueInteger(long reply, long order) { queue( new RESPInteger(reply),order); }

    public void queueDouble(double reply, long order) {queue( new RESPDouble(reply), order); }
    public void queueEmptyArray(long order) { queue(EMPTY_ARRAY,order);}

    public void queue(Collection<String> set,long order) {
        queue(new RESPArray(set),order);
    }
    public TypeStorage getMainStorage() {
        return mainStorage;
    }
    public TypeStorage getStorage(int destinationDb) {
        return myServer.getStorage(destinationDb);
    }
    public void blocking(WorkItem i) {
        blocking.put(i,i.timeout);
    }
    public void doneBlocking(WorkItem i) {
        blocking.remove(i);
    }

    private Set<String> watched = new HashSet<String>();

    public void watch(List<String> keys) {
        watched.addAll(keys);
    }

    public void unwatch() {
        watched.clear();
    }

    private static final Logger LOG = Logger.get(Client.class.getSimpleName());
}
