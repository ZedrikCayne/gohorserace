package com.justaddhippopotamus.ghr.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class Listener extends GoDog {
    private static int clientid = 1;
    @Override
    public void run() {
        super.run();
        Client newClient = null;
        while( running ) {
            try {
                System.out.println("Starting listener on port " + portNumber + ".");
                mainListener = new ServerSocket(portNumber);
                while (running) {
                    Socket clientSocket = mainListener.accept();

                    clientSocket.setKeepAlive(true);
                    clientSocket.setSoLinger(true,0);
                    newClient = new Client( clientSocket, myServer, clientid, myServer.password == null, 0 );
                    ++clientid;
                    myServer.addClient(newClient);
                    newClient.goDogGo();
                }
            } catch (IOException e) {
                //Socket died. Let's try to recover unless we are stopping
            }
        }
        mainLoopCompleted();
    }
    @Override
    public boolean stop() {
        boolean wasRunning = super.stop();
        if( wasRunning ) {
            try {
                if( mainListener != null ) {
                    mainListener.close();
                }
            } catch (Exception e) {
            }
            mainListener = null;
        }
        return wasRunning;
    }

    public Thread goDogGo() {
        myThread = new Thread(this);
        myThread.start();
        return myThread;
    }
    public Listener(int portNumber, Server server) {
        this.portNumber = portNumber;
        this.myServer = server;
    }
    private int portNumber;
    ServerSocket mainListener;
    Thread myThread;
    Server myServer;
}
