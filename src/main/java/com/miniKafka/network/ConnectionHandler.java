package com.minikafka.network;

import com.minikafka.common.Log;
import com.minikafka.metrics.Metrics;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.RequestHeader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Handles one client connection on its own thread with blocking I/O. It reads length-prefixed
 * requests in a loop, hands each to the {@link RequestDispatcher}, and writes back a length-prefixed
 * response carrying the request's correlation id.
 *
 * <p>Wire framing (both directions): {@code INT32 size} followed by {@code size} bytes. For a
 * response those bytes are {@code correlationId (INT32)} + the dispatcher's body.
 */
final class ConnectionHandler implements Runnable {
    private static final Log LOG = Log.of(ConnectionHandler.class);
    private static final int MAX_REQUEST_BYTES = 100 * 1024 * 1024; // 100 MiB guard

    private final Socket socket;
    private final RequestDispatcher dispatcher;
    private final Metrics metrics;

    ConnectionHandler(Socket socket, RequestDispatcher dispatcher, Metrics metrics) {
        this.socket = socket;
        this.dispatcher = dispatcher;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        metrics.connectionOpened();
        try (Socket s = socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {
            while (true) {
                int size;
                try {
                    size = in.readInt();
                } catch (EOFException eof) {
                    break; // client closed cleanly
                }
                if (size <= 0 || size > MAX_REQUEST_BYTES) {
                    LOG.warn("dropping connection: bad request size {}", size);
                    break;
                }
                byte[] frame = new byte[size];
                in.readFully(frame);

                ByteBuffer buf = ByteBuffer.wrap(frame);
                RequestHeader header = RequestHeader.parse(buf);

                byte[] body = dispatcher.dispatch(header, buf);

                ByteWriter resp = new ByteWriter(body.length + 4);
                resp.putInt(header.correlationId());
                resp.putRaw(body);
                byte[] payload = resp.toByteArray();

                out.writeInt(payload.length);
                out.write(payload);
                out.flush();
            }
        } catch (IOException e) {
            // Connection reset / broken pipe: normal when a client goes away.
        } catch (RuntimeException e) {
            LOG.error("unexpected error handling connection", e);
        } finally {
            metrics.connectionClosed();
        }
    }
}