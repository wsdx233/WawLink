package dev.wawlink.protocol

/** Host storage for composer drafts. Updates must be immediate in memory and persist off the UI thread. */
interface ComposerDraftRepository {
    suspend fun load(sessionId: String): RestoredSessionPrompt?
    fun update(sessionId: String, prompt: RestoredSessionPrompt)
}
