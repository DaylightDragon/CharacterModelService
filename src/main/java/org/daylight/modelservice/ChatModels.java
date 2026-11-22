package org.daylight.modelservice;

import java.util.stream.Stream;

public final class ChatModels {
    public enum EventType {
        DELTA,
        DONE,
        ERROR
    }

    public record StreamChunk(EventType type, String text, String responseId) {
        public static StreamChunk delta(String text) {
            return new StreamChunk(EventType.DELTA, text, null);
        }

        public static StreamChunk done(String responseId) {
            return new StreamChunk(EventType.DONE, "", responseId);
        }

        public static StreamChunk error(String message) {
            return new StreamChunk(EventType.ERROR, message, null);
        }
    }

    /**
     * Результат обращения к высокоуровневому сервису:
     * либо можно читать стрим чанков, либо модель сейчас занята.
     */
    public sealed interface AskOutcome permits AskOutcome.Busy, AskOutcome.StreamReady {
        record Busy(String message) implements AskOutcome {}

        record StreamReady(
                String conversationId,
                Stream<StreamChunk> chunks,
                Runnable onRelease // обязательно вызвать по завершении/обрыве стрима
        ) implements AskOutcome {}
    }
}
