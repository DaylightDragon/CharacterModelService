package org.daylight.modelservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ConversationRegistry {
    private final Map<String, String> lastResponseIds = new ConcurrentHashMap<>();
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
        String id = lastResponseIds.get(conversationId);
        return (id == null || id.isBlank()) ? Optional.empty() : Optional.of(id);
    }

    public void updateResponseId(String conversationId, String newResponseId) {
        if (conversationId != null && !conversationId.isBlank()
                && newResponseId != null && !newResponseId.isBlank()) {
            lastResponseIds.put(conversationId, newResponseId);
            saveToDisk();
        }
    }

    public boolean resetConversation(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) return false;
        boolean removed = lastResponseIds.remove(conversationId) != null;
        if (removed) {
            saveToDisk();
        }
        return removed;
    }

    public void resetAll() {
        lastResponseIds.clear();
        saveToDisk();
    }

    private void loadFromDisk() {
        synchronized (fileLock) {
            if (!Files.exists(storagePath)) return;
            try {
                Map<String, String> loaded = mapper.readValue(
                        storagePath.toFile(),
                        new TypeReference<>() {}
                );
                if (loaded != null) {
                    lastResponseIds.putAll(loaded);
                }
            } catch (IOException e) {
                System.err.println("Не удалось загрузить историю диалогов из " + storagePath + ": " + e.getMessage());
            }
        }
    }

    private void saveToDisk() {
        synchronized (fileLock) {
            try {
                if (storagePath.getParent() != null) {
                    Files.createDirectories(storagePath.getParent());
                }
                Path tempFile = storagePath.resolveSibling(storagePath.getFileName() + ".tmp");
                mapper.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), lastResponseIds);
                Files.move(tempFile, storagePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                System.err.println("Не удалось сохранить историю диалогов в " + storagePath + ": " + e.getMessage());
            }
        }
    }
}
