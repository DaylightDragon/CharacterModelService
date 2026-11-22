package org.daylight.modelservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConversationRegistry {
    public record StepRecord(long messageId, String responseId) {}

    public record ConversationState(String lastResponseId, List<StepRecord> steps) {
        public ConversationState {
            steps = (steps == null) ? new ArrayList<>() : new ArrayList<>(steps);
        }
    }

    private final Map<String, ConversationState> states = new ConcurrentHashMap<>();
    private final Path storagePath;
    private final ObjectMapper mapper;
    private final Object fileLock = new Object();

    public ConversationRegistry(Path storagePath, ObjectMapper mapper) {
        this.storagePath = storagePath;
        this.mapper = mapper;
        loadFromDisk();
    }

    public Optional<String> getLastResponseId(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) return Optional.empty();
        conversationId = conversationId;
        ConversationState state = states.get(conversationId);
        if (state == null || state.lastResponseId() == null || state.lastResponseId().isBlank()) {
            return Optional.empty();
        }
        return Optional.of(state.lastResponseId());
    }

    /**
     * Обновляет responseId после завершения стрима и привязывает его к messageId (если передан > 0).
     */
    public void recordResponse(String conversationId, Long messageId, String newResponseId) {
        if (conversationId == null || conversationId.isBlank()
                || newResponseId == null || newResponseId.isBlank()) {
            return;
        }

        synchronized (fileLock) {
            ConversationState current = states.computeIfAbsent(
                    conversationId,
                    k -> new ConversationState(null, new ArrayList<>())
            );
            List<StepRecord> updatedSteps = new ArrayList<>(current.steps());
            if (messageId != null && messageId > 0) {
                // Если по этому messageId уже была запись — обновим её, иначе добавим в конец
                int existingIdx = -1;
                for (int i = 0; i < updatedSteps.size(); i++) {
                    if (updatedSteps.get(i).messageId() == messageId) {
                        existingIdx = i;
                        break;
                    }
                }
                StepRecord record = new StepRecord(messageId, newResponseId);
                if (existingIdx >= 0) {
                    updatedSteps.set(existingIdx, record);
                } else {
                    updatedSteps.add(record);
                }
            }
            states.put(conversationId, new ConversationState(newResponseId, updatedSteps));
            saveToDiskLocked();
        }
    }

    /**
     * Откатывает диалог к состоянию ДО указанного messageId:
     * удаляет само сообщение messageId и все последующие шаги.
     */
    public ChatModels.RollbackResult rollbackBeforeMessage(String conversationId, long targetMessageId) {
        if (conversationId == null || conversationId.isBlank()) {
            return new ChatModels.RollbackResult(false, conversationId, targetMessageId, null, 0);
        }

        synchronized (fileLock) {
            ConversationState current = states.get(conversationId);
            if (current == null || current.steps().isEmpty()) {
                return new ChatModels.RollbackResult(false, conversationId, targetMessageId, null, 0);
            }

            List<StepRecord> steps = new ArrayList<>(current.steps());
            int foundIdx = -1;
            for (int i = 0; i < steps.size(); i++) {
                if (steps.get(i).messageId() == targetMessageId) {
                    foundIdx = i;
                    break;
                }
            }

            if (foundIdx < 0) {
                return new ChatModels.RollbackResult(
                        false,
                        conversationId,
                        targetMessageId,
                        current.lastResponseId(),
                        0
                );
            }

            int removedCount = steps.size() - foundIdx;
            steps.subList(foundIdx, steps.size()).clear();

            String newLastResponseId = steps.isEmpty()
                    ? null
                    : steps.get(steps.size() - 1).responseId();

            states.put(conversationId, new ConversationState(newLastResponseId, steps));
            saveToDiskLocked();

            return new ChatModels.RollbackResult(
                    true,
                    conversationId,
                    targetMessageId,
                    newLastResponseId,
                    removedCount
            );
        }
    }

    /**
     * Сбрасывает текущий responseId (например, если узел не найден на текущем сервере LM Studio).
     * Шаги при этом не теряются — при откате можно будет попробовать более ранний узел.
     */
    public void invalidateCurrentResponseId(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) return;
        synchronized (fileLock) {
            ConversationState current = states.get(conversationId);
            if (current != null && current.lastResponseId() != null) {
                states.put(conversationId, new ConversationState(null, current.steps()));
                saveToDiskLocked();
            }
        }
    }

    public boolean resetConversation(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) return false;
        synchronized (fileLock) {
            boolean removed = states.remove(conversationId) != null;
            if (removed) {
                saveToDiskLocked();
            }
            return removed;
        }
    }

    public void resetAll() {
        synchronized (fileLock) {
            states.clear();
            saveToDiskLocked();
        }
    }

    private void loadFromDisk() {
        synchronized (fileLock) {
            if (!Files.exists(storagePath)) return;
            try {
                JsonNode root = mapper.readTree(storagePath.toFile());
                if (root == null || !root.isObject()) return;

                Set<Map.Entry<String, JsonNode>> fields = root.properties();
                boolean migrated = false;

                for (Map.Entry<String, JsonNode> entry : root.properties()) {
                    String convId = entry.getKey();
                    JsonNode val = entry.getValue();

                    if (val.isTextual()) {
                        // Старый формат: "convId": "resp_..."
                        String respId = val.asText();
                        states.put(convId, new ConversationState(respId, new ArrayList<>()));
                        migrated = true;
                    } else if (val.isObject()) {
                        // Новый формат: "convId": { "lastResponseId": "...", "steps": [...] }
                        ConversationState state = mapper.treeToValue(val, ConversationState.class);
                        if (state != null) {
                            states.put(convId, state);
                        }
                    }
                }

                if (migrated) {
                    saveToDiskLocked();
                }
            } catch (IOException e) {
                System.err.println("Не удалось загрузить реестр диалогов из " + storagePath + ": " + e.getMessage());
            }
        }
    }

    private void saveToDiskLocked() {
        try {
            if (storagePath.getParent() != null) {
                Files.createDirectories(storagePath.getParent());
            }
            Path tempFile = storagePath.resolveSibling(storagePath.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), states);
            Files.move(tempFile, storagePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Не удалось сохранить реестр диалогов в " + storagePath + ": " + e.getMessage());
        }
    }
}
