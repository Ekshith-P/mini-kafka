package com.minikafka.network;

import com.minikafka.common.Log;
import com.minikafka.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The broker's front door. One acceptor thread runs the {@link ServerSocket#accept()} loop and hands
 * each new connection to a {@link ConnectionHandler} on a cached thread pool (one thread per active
 * connection).
 *
 * <p>This blocking, thread-per-connection design is simple to reason about and plenty fast for tens
 * to hundreds of connections. Real Kafka instead uses a non-blocking NIO reactor (a few selector
 * threads multiplexing thousands of sockets); that - or Java 21 virtual threads here - is the natural
 * next step for much higher connection counts.
 */
public final class KafkaServer {
    private static final Log LOG = Log.of(KafkaServer.class);

    private final int port;
    private final RequestDispatcher dispatcher;
    private final Metrics metrics;

    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptorThread;
    private ExecutorService connectionPool;

    public KafkaServer(int port, RequestDispatcher dispatcher, Metrics metrics) {
        this.port = port;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
    }

    public void start() {
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to bind port " + port, e);
        }

        AtomicInteger counter = new AtomicInteger();
        connectionPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "mk-conn-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        });

        running = true;
        acceptorThread = new Thread(this::acceptLoop, "mk-acceptor-" + port);
        acceptorThread.start();
        LOG.info("listening on port {}", port);
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                connectionPool.submit(new ConnectionHandler(socket, dispatcher, metrics));
            } catch (SocketException e) {
                if (running) {
                    LOG.warn("accept failed: {}", e.toString());
                }
                // else: socket closed during shutdown - expected
            } catch (IOException e) {
                LOG.warn("accept error: {}", e.toString());
            }
        }
    }

    public int port() {
        return port;
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOG.warn("error closing server socket: {}", e.toString());
        }
        if (connectionPool != null) {
            connectionPool.shutdownNow();
            try {
                connectionPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("stopped listening on port {}", port);
    }
}