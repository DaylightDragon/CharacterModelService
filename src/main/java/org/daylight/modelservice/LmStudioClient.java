package org.daylight.modelservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LmStudioClient {
    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String endpointUrl;
    private final String model;
    private final String reasoning;
    private final String systemPrompt;
    private final Integer maxOutputTokens = 300;

    public LmStudioClient(String endpointUrl, String model, ObjectMapper mapper) {
        this(endpointUrl, model, "on", null, mapper);
    }

    public LmStudioClient(
            String endpointUrl,
            String model,
            String reasoning,
            String systemPrompt,
            ObjectMapper mapper
    ) {
        this.endpointUrl = endpointUrl;
        this.model = model;
        this.reasoning = reasoning;
        this.systemPrompt = systemPrompt;
        this.mapper = mapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public Stream<ChatModels.StreamChunk> streamChat(
            String prompt,
            String previousResponseId,
            Consumer<String> onResponseIdCompleted,
            Runnable onInvalidPreviousId
    ) {
        System.out.println("Sending prompt: " + prompt);

        boolean hasPrevId = previousResponseId != null && !previousResponseId.isBlank();

        ObjectNode payload = mapper.createObjectNode();
        payload.put("model", model);
        payload.put("input", prompt);
        payload.put("stream", true);
//        payload.put("max_output_tokens", 150);

        if (reasoning != null && !reasoning.isBlank()) {
            payload.put("reasoning", reasoning);
        }
        if (!hasPrevId && systemPrompt != null && !systemPrompt.isBlank()) {
            payload.put("system_prompt", systemPrompt);
        }
        if (hasPrevId) {
            payload.put("previous_response_id", previousResponseId);
        }

        try {
            String jsonBody = mapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpointUrl))
                    .header("Content-Type", "application/json")
                    .header("Accept", "text/event-stream")
                    .timeout(Duration.ofMinutes(5))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<Stream<String>> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofLines());

            int status = response.statusCode();

            // Если LM Studio отклонил previous_response_id (не найден / устарел) — сбрасываем и повторяем начисто
            if (hasPrevId && (status == 400 || status == 404)) {
                try (Stream<String> ignored = response.body()) {
                    // Закрываем тело ошибочного ответа
                }
                if (onInvalidPreviousId != null) {
                    onInvalidPreviousId.run();
                }
                return streamChat(prompt, null, onResponseIdCompleted, null);
            }

            if (status >= 400) {
                try (Stream<String> ignored = response.body()) {
                    return Stream.of(ChatModels.StreamChunk.error("LM Studio HTTP " + status));
                }
            }

            Stream<String> rawLines = response.body();
            Iterator<ChatModels.StreamChunk> chunkIterator = new Iterator<>() {
                private final Iterator<String> lineIt = rawLines.iterator();
                private ChatModels.StreamChunk nextChunk;
                private boolean finished = false;

                @Override
                public boolean hasNext() {
                    if (nextChunk != null) return true;
                    if (finished) return false;

                    while (lineIt.hasNext()) {
                        String line = lineIt.next();
                        if (!line.startsWith("data: ")) continue;
                        String rawJson = line.substring(6).trim();
                        if (rawJson.isEmpty() || "[DONE]".equals(rawJson)) continue;

                        try {
                            JsonNode event = mapper.readTree(rawJson);
                            String type = event.path("type").asText("");

                            if ("message.delta".equals(type)) {
                                String delta = event.path("content").asText("");
                                if (!delta.isEmpty()) {
                                    nextChunk = ChatModels.StreamChunk.delta(delta);
                                    return true;
                                }
                            } else if ("chat.end".equals(type)) {
                                String responseId = event.path("result").path("response_id").asText(null);
                                if (responseId != null && !responseId.isBlank() && onResponseIdCompleted != null) {
                                    onResponseIdCompleted.accept(responseId);
                                }
                                nextChunk = ChatModels.StreamChunk.done(responseId);
                                finished = true;
                                return true;
                            }
                        } catch (Exception e) {
                            nextChunk = ChatModels.StreamChunk.error("Parse error: " + e.getMessage());
                            finished = true;
                            return true;
                        }
                    }

                    finished = true;
                    return false;
                }

                @Override
                public ChatModels.StreamChunk next() {
                    ChatModels.StreamChunk current = nextChunk;
                    nextChunk = null;
                    return current;
                }
            };

            Stream<ChatModels.StreamChunk> resultStream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(chunkIterator, Spliterator.ORDERED),
                    false
            );
            return resultStream.onClose(rawLines::close);

        } catch (Exception e) {
            return Stream.of(ChatModels.StreamChunk.error("Request failed: " + e.getMessage()));
        }
    }
}
