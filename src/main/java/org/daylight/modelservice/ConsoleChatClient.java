package org.daylight.modelservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;

public class ConsoleChatClient {
    private static final String BASE_URL = "http://localhost:8080/api/chat";

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        // Фиксированный или сгенерированный ID диалога на время консольной сессии
        String conversationId = "cli-" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("Диалог запущен. conversationId=" + conversationId);
        System.out.println("Введите сообщение и нажмите Enter (команда /exit — выход, /new — новый диалог):\n");

        try (BufferedReader console = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            while (true) {
                System.out.print("Вы> ");
                String line = console.readLine();
                if (line == null) break;

                String prompt = line.trim();
                if (prompt.isEmpty()) continue;
                if ("/exit".equalsIgnoreCase(prompt)) {
                    System.out.println("Выход.");
                    break;
                }
                if ("/new".equalsIgnoreCase(prompt)) {
                    conversationId = "cli-" + UUID.randomUUID().toString().substring(0, 8);
                    System.out.println("Новый диалог: conversationId=" + conversationId);
                    continue;
                }

                sendAndStream(httpClient, mapper, conversationId, prompt);
            }
        }
    }

    private static void sendAndStream(
            HttpClient httpClient,
            ObjectMapper mapper,
            String conversationId,
            String prompt
    ) {
        String encodedConvId = URLEncoder.encode(conversationId, StandardCharsets.UTF_8);
        String encodedPrompt = URLEncoder.encode(prompt, StandardCharsets.UTF_8);
        String url = BASE_URL + "?conversationId=" + encodedConvId + "&prompt=" + encodedPrompt;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofMinutes(5))
                .GET()
                .build();

        System.out.print("Модель> ");

        try {
            HttpResponse<Stream<String>> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofLines());

            int status = response.statusCode();
            if (status == 429) {
                try (Stream<String> lines = response.body()) {
                    String bodyText = lines.reduce("", (a, b) -> a + b);
                    JsonNode errNode = bodyText.isBlank() ? null : mapper.readTree(bodyText);
                    String msg = (errNode != null && errNode.hasNonNull("message"))
                            ? errNode.get("message").asText()
                            : "Модель сейчас занята (HTTP 429)";
                    System.out.println("\n[ЗАЯТО] " + msg);
                }
                return;
            }

            if (status >= 400) {
                try (Stream<String> lines = response.body()) {
                    String errBody = lines.reduce("", (a, b) -> a + b);
                    System.out.println("\n[HTTP " + status + "] " + errBody);
                }
                return;
            }

            try (Stream<String> lines = response.body()) {
                for (String jsonLine : (Iterable<String>) lines::iterator) {
                    if (jsonLine.isBlank()) continue;

                    JsonNode chunk = mapper.readTree(jsonLine);
                    String type = chunk.path("type").asText("");
                    String text = chunk.path("text").asText("");

                    switch (type) {
                        case "delta" -> {
                            System.out.print(text);
                            System.out.flush();
                        }
                        case "done" -> {
                            String respId = chunk.path("responseId").asText("");
                            System.out.println();
                            System.out.println("[DONE | responseId=" + respId + "]");
                        }
                        case "error" -> {
                            System.out.println();
                            System.out.println("[ERROR] " + text);
                        }
                        default -> System.out.println("\n[" + type + "] " + chunk);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("\n[СБОЙ ПОТОКА] " + e.getMessage());
        }
    }
}
