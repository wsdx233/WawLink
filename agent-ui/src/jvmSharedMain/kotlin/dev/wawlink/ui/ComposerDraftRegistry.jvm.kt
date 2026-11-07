package dev.wawlink.ui

import dev.wawlink.protocol.RestoredSessionPrompt

actual class ComposerDraftRegistry actual constructor() {
    private val lock = Any()
    private val drafts = mutableMapOf<String, ComposerDraftState>()
    private var closed = false

    internal actual fun getOrCreate(sessionId: String, create: () -> ComposerDraftState): ComposerDraftState = synchronized(lock) {
        // A late composition after host shutdown must not retain another session entry.
        if (closed) create() else drafts.getOrPut(sessionId, create)
    }

    internal actual fun bindPersistence(sessionId: String, draft: ComposerDraftState, persist: ((RestoredSessionPrompt) -> Unit)?) {
        synchronized(lock) {
            if (!closed && drafts[sessionId] === draft) draft.persistRestoration = persist
        }
    }

    actual fun discard(sessionId: String) {
        synchronized(lock) { drafts.remove(sessionId)?.persistRestoration = null }
    }

    actual fun close() {
        synchronized(lock) {
            closed = true
            drafts.values.forEach { it.persistRestoration = null }
            drafts.clear()
        }
    }
}
