package dev.wawlink.app

import dev.wawlink.protocol.*
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import kotlinx.coroutines.*
import kotlinx.serialization.json.*

/** App-private drafts are independent of native history and never restore a controller or start a container. */
internal class SessionDraftStore(
    directory: File,
    private val canPersist: (String) -> Boolean,
    private val onError: (Throwable) -> Unit,
    private val writeFile: (File, String) -> Unit = { target, content -> target.writeText(content) },
) : ComposerDraftRepository, AutoCloseable {
    private val directory = File(directory, "drafts")
    /** UI callers only use this short memory lock; filesystem work never holds it. */
    private val lock = Any()
    private val ioLock = Any()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private data class Draft(val prompt: RestoredSessionPrompt, val revision: Long)
    private val drafts = mutableMapOf<String, Draft>()
    private val pending = mutableMapOf<String, Job>()
    private val removed = mutableSetOf<String>()
    private var nextRevision = 0L
    private var closed = false

    private fun file(id: String): File {
        val name = MessageDigest.getInstance("SHA-256").digest(id.toByteArray()).joinToString("") { "%02x".format(it) }
        return File(directory, "$name.json")
    }

    override suspend fun load(sessionId: String): RestoredSessionPrompt? = withContext(Dispatchers.IO) {
        synchronized(lock) {
            if (closed || sessionId in removed) return@withContext null
            drafts[sessionId]?.let { return@withContext it.prompt }
        }
        if (!canPersist(sessionId)) return@withContext null
        try {
            synchronized(ioLock) {
                // Another load, typing, deletion or close may have won while disk work was queued.
                synchronized(lock) {
                    if (closed || sessionId in removed) return@withContext null
                    drafts[sessionId]?.let { return@withContext it.prompt }
                }
                val source = file(sessionId)
                if (!source.exists()) return@withContext null
                val saved = Json.parseToJsonElement(source.readText()).jsonObject
                val prompt = RestoredSessionPrompt(saved.getValue("text").jsonPrimitive.content,
                    saved["attachments"]?.jsonArray.orEmpty().map { value ->
                        val attachment = value.jsonObject
                        AttachmentFile(attachment.getValue("id").jsonPrimitive.content, attachment.getValue("name").jsonPrimitive.content,
                            attachment.getValue("path").jsonPrimitive.content, attachment.getValue("mimeType").jsonPrimitive.content,
                            attachment.getValue("sizeBytes").jsonPrimitive.long, AttachmentSource.valueOf(attachment.getValue("source").jsonPrimitive.content))
                    })
                synchronized(lock) {
                    if (closed || sessionId in removed) return@withContext null
                    // A newer in-memory value includes an explicitly cleared draft.
                    drafts.getOrPut(sessionId) { Draft(prompt, ++nextRevision) }.prompt
                }
            }
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) {
            onError(IllegalStateException("无法恢复会话草稿", failure))
            null
        }
    }

    override fun update(sessionId: String, prompt: RestoredSessionPrompt) = synchronized(lock) {
        if (closed || sessionId in removed || drafts[sessionId]?.prompt == prompt) return@synchronized
        drafts[sessionId] = Draft(prompt.copy(attachments = prompt.attachments.toList()), ++nextRevision)
        pending.remove(sessionId)?.cancel()
        pending[sessionId] = scope.launch {
            val owner = currentCoroutineContext()[Job]
            try {
                if (prompt.text.isNotEmpty() || prompt.attachments.isNotEmpty()) delay(300)
                persist(sessionId)
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (failure: Exception) { onError(IllegalStateException("无法保存会话草稿", failure)) }
            finally {
                synchronized(lock) { if (pending[sessionId] === owner) pending.remove(sessionId) }
            }
        }
        Unit
    }

    fun discard(sessionId: String) = synchronized(lock) {
        if (closed) return@synchronized
        removed.add(sessionId)
        pending.remove(sessionId)?.cancel()
        drafts.remove(sessionId)
        // The I/O lock orders this delete after any in-flight atomic replacement.
        scope.launch {
            try { persist(sessionId) }
            catch (failure: Exception) { onError(IllegalStateException("无法清除会话草稿", failure)) }
        }
        Unit
    }

    private fun persist(id: String, flushOnClose: Boolean = false): Unit = synchronized(ioLock) {
        val snapshot = synchronized(lock) {
            if (closed && !flushOnClose) return@synchronized null
            if (id in removed) null else drafts[id]
        }
        val deleted = synchronized(lock) { id in removed }
        if (synchronized(lock) { closed && !flushOnClose }) return
        val target = file(id)
        if (deleted) { Files.deleteIfExists(target.toPath()); return }
        if (!canPersist(id)) return
        val prompt = snapshot?.prompt ?: return
        if (prompt.text.isEmpty() && prompt.attachments.isEmpty()) { Files.deleteIfExists(target.toPath()); return }
        check(directory.isDirectory || directory.mkdirs()) { "无法创建草稿目录" }
        val text = buildJsonObject {
            put("text", prompt.text)
            put("attachments", buildJsonArray { prompt.attachments.forEach { attachment -> add(buildJsonObject {
                put("id", attachment.id); put("name", attachment.name); put("path", attachment.path)
                put("mimeType", attachment.mimeType); put("sizeBytes", attachment.sizeBytes); put("source", attachment.source.name)
            }) } })
        }.toString()
        val temporary = Files.createTempFile(directory.toPath(), "draft-", ".tmp")
        try {
            writeFile(temporary.toFile(), text)
            val current = synchronized(lock) {
                (flushOnClose || !closed) && id !in removed && drafts[id]?.revision == snapshot.revision
            }
            if (current && canPersist(id)) {
                try { Files.move(temporary, target.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING) }
                catch (_: java.nio.file.AtomicMoveNotSupportedException) { Files.move(temporary, target.toPath(), StandardCopyOption.REPLACE_EXISTING) }
            }
        } finally { Files.deleteIfExists(temporary) }
    }

    override fun close() {
        val ids = synchronized(lock) {
            if (closed) return
            closed = true
            pending.values.forEach { it.cancel() }
            pending.clear()
            (drafts.keys + removed).toList()
        }
        scope.cancel()
        // Closing may synchronously flush, but a UI update never waits on this disk lock.
        ids.forEach { id -> try { persist(id, flushOnClose = true) } catch (failure: Exception) { onError(failure) } }
    }
}
