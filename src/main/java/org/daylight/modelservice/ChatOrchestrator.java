package org.daylight.modelservice;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class ChatOrchestrator {
    private final LmStudioClient lmClient;
    private final ConversationRegistry registry;

    private final AtomicBoolean modelBusy = new AtomicBoolean(false);
    private final Set<String> activeConversations = ConcurrentHashMap.newKeySet();

    public ChatOrchestrator(LmStudioClient lmClient, ConversationRegistry registry) {
        this.lmClient = lmClient;
        this.registry = registry;
    }

    public ChatModels.AskOutcome askStream(String conversationId, String prompt) {
        return askStream(conversationId, prompt, null);
    }

    public ChatModels.AskOutcome askStream(String conversationId, String prompt, Long messageId) {
        String effectiveConvId = (conversationId == null || conversationId.isBlank())
                ? UUID.randomUUID().toString()
                : conversationId;

        if (!modelBusy.compareAndSet(false, true)) {
            return new ChatModels.AskOutcome.Busy("Модель сейчас занята, попробуйте чуть позже.");
        }

        if (!activeConversations.add(effectiveConvId)) {
            modelBusy.set(false);
            return new ChatModels.AskOutcome.Busy("Этот диалог уже генерирует ответ.");
        }

        AtomicBoolean released = new AtomicBoolean(false);
        Runnable releaseLock = () -> {
            if (released.compareAndSet(false, true)) {
                activeConversations.remove(effectiveConvId);
                modelBusy.set(false);
            }
        };

        try {
            String prevId = registry.getLastResponseId(effectiveConvId).orElse(null);

            Stream<ChatModels.StreamChunk> stream = lmClient.streamChat(
                    prompt,
                    prevId,
                    newResponseId -> registry.recordResponse(effectiveConvId, messageId, newResponseId),
                    () -> {
                        System.out.println("[Orchestrator] Узел не найден на текущем сервере, сбрасываю текущий ID для: " + effectiveConvId);
                        registry.invalidateCurrentResponseId(effectiveConvId);
                    }
            ).onClose(releaseLock);

            return new ChatModels.AskOutcome.StreamReady(effectiveConvId, stream, releaseLock);
        } catch (Exception e) {
            releaseLock.run();
            throw e;
        }
    }

    public ChatModels.RollbackResult rollbackBeforeMessage(String conversationId, long messageId) {
        return registry.rollbackBeforeMessage(conversationId, messageId);
    }

    public boolean resetConversation(String conversationId) {
        return registry.resetConversation(conversationId);
    }

    public void resetAllConversations() {
        registry.resetAll();
    }
}
