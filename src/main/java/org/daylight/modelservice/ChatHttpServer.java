package org.daylight.modelservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class ChatHttpServer {
    private static final String LM_STUDIO_URL = "http://localhost:1235/api/v1/chat";
    private static final String MODEL = "unsloth/qwen3.5-9b@q3_k_s";
    private static final Path REGISTRY_FILE = Path.of("data", "conversations.json");
    private static final int PORT = 5918;

    private final ObjectMapper mapper = new ObjectMapper();
    private final ChatOrchestrator orchestrator;

    public ChatHttpServer(ChatOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        LmStudioClient client = new LmStudioClient(LM_STUDIO_URL, MODEL, mapper);
        ConversationRegistry registry = new ConversationRegistry(REGISTRY_FILE, mapper);
        ChatOrchestrator orchestrator = new ChatOrchestrator(client, registry);

        ChatHttpServer chatServer = new ChatHttpServer(orchestrator);

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/api/chat", chatServer::handleChat);
        server.createContext("/api/chat/rollback", chatServer::handleRollback);
        server.createContext("/api/chat/reset", chatServer::handleReset);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("Server running on http://localhost:" + PORT + "/api/chat");
        System.out.println("Registry file path: " + REGISTRY_FILE.toAbsolutePath());
    }

    private void handleChat(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
        String conversationId = params.get("conversationId");
        String prompt = params.getOrDefault("prompt", "");
        Long messageId = parseLongOrNull(params.get("messageId"));

        if (prompt.isBlank()) {
            sendJson(exchange, 400, "{\"error\":\"prompt is required\"}");
            return;
        }

        ChatModels.AskOutcome outcome = orchestrator.askStream(conversationId, prompt, messageId);

        switch (outcome) {
            case ChatModels.AskOutcome.Busy busy -> {
                ObjectNode err = mapper.createObjectNode();
                err.put("status", "busy");
                err.put("message", busy.message());
                sendJson(exchange, 429, mapper.writeValueAsString(err));
            }
            case ChatModels.AskOutcome.StreamReady ready -> {
                Headers headers = exchange.getResponseHeaders();
                headers.set("Content-Type", "application/x-ndjson; charset=utf-8");
                headers.set("X-Conversation-Id", ready.conversationId());
                headers.set("Cache-Control", "no-cache");
                exchange.sendResponseHeaders(200, 0);

                try (OutputStream out = exchange.getResponseBody();
                        var stream = ready.chunks()) {
                    for (ChatModels.StreamChunk chunk : (Iterable<ChatModels.StreamChunk>) stream::iterator) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("type", chunk.type().name().toLowerCase());
                        node.put("conversationId", ready.conversationId());
                        node.put("text", chunk.text());
                        if (chunk.responseId() != null) {
                            node.put("responseId", chunk.responseId());
                        }
                        out.write((mapper.writeValueAsString(node) + "\n").getBytes(StandardCharsets.UTF_8));
                        out.flush();
                    }
                } catch (IOException ignored) {
                } finally {
                    ready.onRelease().run();
                    exchange.close();
                }
            }
        }
    }

    /**
     * Откат до указанного сообщения:
     * /api/chat/rollback?conversationId=discord-channel-...&beforeMessageId=123456789
     */
    private void handleRollback(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod()) && !"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
        String conversationId = params.get("conversationId");
        Long beforeMessageId = parseLongOrNull(params.get("beforeMessageId"));

        if (conversationId == null || conversationId.isBlank() || beforeMessageId == null) {
            sendJson(exchange, 400, "{\"error\":\"conversationId and beforeMessageId are required\"}");
            return;
        }

        ChatModels.RollbackResult res = orchestrator.rollbackBeforeMessage(conversationId, beforeMessageId);
        ObjectNode body = mapper.createObjectNode();
        body.put("status", res.found() ? "ok" : "not_found");
        body.put("conversationId", res.conversationId());
        body.put("beforeMessageId", res.targetMessageId());
        body.put("removedSteps", res.removedSteps());
        if (res.activeResponseId() != null) {
            body.put("activeResponseId", res.activeResponseId());
        } else {
            body.putNull("activeResponseId");
        }
        sendJson(exchange, res.found() ? 200 : 404, mapper.writeValueAsString(body));
    }

    private void handleReset(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod()) && !"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
        String all = params.getOrDefault("all", "false");
        String conversationId = params.get("conversationId");

        ObjectNode resp = mapper.createObjectNode();
        if ("true".equalsIgnoreCase(all)) {
            orchestrator.resetAllConversations();
            resp.put("status", "ok");
            resp.put("scope", "all");
            sendJson(exchange, 200, mapper.writeValueAsString(resp));
            return;
        }

        if (conversationId == null || conversationId.isBlank()) {
            sendJson(exchange, 400, "{\"error\":\"Provide conversationId or all=true\"}");
            return;
        }

        boolean removed = orchestrator.resetConversation(conversationId);
        resp.put("status", "ok");
        resp.put("conversationId", conversationId);
        resp.put("existed", removed);
        sendJson(exchange, 200, mapper.writeValueAsString(resp));
    }

    private static Long parseLongOrNull(String val) {
        if (val == null || val.isBlank()) return null;
        try {
            return Long.parseLong(val.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void sendJson(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> map = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) return map;
        for (String pair : rawQuery.split("&")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                String k = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                map.put(k, v);
            }
        }
        return map;
    }
}
