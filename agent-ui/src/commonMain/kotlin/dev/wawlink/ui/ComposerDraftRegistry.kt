package dev.wawlink.ui

import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.staticCompositionLocalOf
import androidx.compose.runtime.toMutableStateList
import androidx.compose.runtime.saveable.listSaver
import dev.wawlink.protocol.AttachmentFile
import dev.wawlink.protocol.AttachmentSource
import dev.wawlink.protocol.RestoredSessionPrompt

/** Host-owned composer identities survive page disposal and Activity recreation. */
expect class ComposerDraftRegistry() {
    internal fun getOrCreate(sessionId: String, create: () -> ComposerDraftState): ComposerDraftState
    internal fun bindPersistence(sessionId: String, draft: ComposerDraftState, persist: ((RestoredSessionPrompt) -> Unit)?)
    fun discard(sessionId: String)
    fun close()
}

val LocalComposerDraftRegistry = staticCompositionLocalOf<ComposerDraftRegistry?> { null }

internal fun composerDraftForSession(
    sessionId: String,
    registry: ComposerDraftRegistry?,
    initial: () -> RestoredSessionPrompt = { RestoredSessionPrompt("", emptyList()) },
): ComposerDraftState {
    val create = {
        val prompt = initial()
        ComposerDraftState(mutableStateOf(prompt.text), prompt.attachments.toMutableStateList())
    }
    return registry?.getOrCreate(sessionId, create) ?: create()
}

internal fun composerDraftSaver(sessionId: String, registry: ComposerDraftRegistry?) = listSaver<ComposerDraftState, Any>(
    save = { draft ->
        val prompt = draft.snapshot()
        listOf(sessionId, prompt.text, prompt.attachments.map {
            listOf(it.id, it.name, it.path, it.mimeType, it.sizeBytes, it.source.name)
        })
    },
    restore = { rows ->
        if (rows[0] != sessionId) null
        else composerDraftForSession(sessionId, registry) {
            RestoredSessionPrompt(rows[1] as String, (rows[2] as List<*>).map { row ->
                val fields = row as List<*>
                AttachmentFile(fields[0] as String, fields[1] as String, fields[2] as String, fields[3] as String,
                    (fields[4] as Number).toLong(), when (val source = fields[5]) {
                        is Boolean -> if (source) AttachmentSource.WORKSPACE else AttachmentSource.LOCAL
                        else -> AttachmentSource.valueOf(source as String)
                    })
            })
        }
    },
)
