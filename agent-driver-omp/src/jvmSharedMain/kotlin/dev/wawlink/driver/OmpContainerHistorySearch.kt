package dev.wawlink.driver

import dev.wawlink.runtime.withContainerFileOperation
import java.io.File
import java.io.IOException
import java.security.MessageDigest
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.*

data class ContainerHistorySearchMatch(
    val appSessionId: String,
    val nativeSessionId: String,
    val entryId: String,
    val sessionFile: String,
    val role: String,
    val entryType: String,
    val text: String,
    val matchStart: Int,
    val matchEnd: Int,
)

sealed interface ContainerHistorySearchUpdate {
    data class Match(val value: ContainerHistorySearchMatch) : ContainerHistorySearchUpdate
    data class Progress(val completed: Int, val total: Int) : ContainerHistorySearchUpdate
    data class Failure(val appSessionId: String, val message: String) : ContainerHistorySearchUpdate
    data object Complete : ContainerHistorySearchUpdate
}

/** Explicit metadata-scoped lookup. Only this request starts a guest file worker; no chat is restored. */
fun OmpProcessPool.searchSessionContents(
    containerId: String,
    sessionIds: List<String>,
    query: String,
): Flow<ContainerHistorySearchUpdate> = channelFlow {
    val value = query.trim()
    require(value.isNotEmpty() && value.length <= 256) { "请输入不超过 256 字的查找内容" }
    val ids = sessionIds.distinct()
    val expectedStopGeneration = runtime.containers.value.firstOrNull { it.config.id == containerId }?.stopGeneration
        ?: throw OmpProcessStoppedException()
    val (agentDirectory, targets) = withContext(Dispatchers.IO) {
        val directory = guestAgentDirectory(containerId)
        directory to ids.map { id ->
            val key = MessageDigest.getInstance("SHA-256").digest(id.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
            val metadata = File(dataDir, "omp-sessions/$key.json")
            check(metadata.isFile) { "会话元数据已不存在，请重新查找" }
            val record = DriverJson.parseToJsonElement(metadata.readText()).jsonObject
            check(record["id"]?.jsonPrimitive?.content == id && record["containerId"]?.jsonPrimitive?.content == containerId) { "会话所属容器已变化，请重新查找" }
            val boundFile = record["sessionFile"]?.jsonPrimitive?.contentOrNull
            if (boundFile != null) {
                val prefix = "$directory/wawlink/sessions/$key/"
                check(boundFile.startsWith(prefix) && boundFile.removePrefix(prefix).let { !it.contains('/') && it.endsWith(".jsonl") && it != ".jsonl" }) { "会话绑定的历史文件越界，请检查会话元数据" }
            }
            buildJsonObject { put("id", id); put("sessionFile", boundFile) }
        }
    }
    if (ids.isEmpty()) { send(ContainerHistorySearchUpdate.Complete); return@channelFlow }
    val guest = OmpGuestFiles(runtime, containerId)
    var process: Process? = null
    var reader: Deferred<Unit>? = null
    var stderr: Job? = null
    var failure: Throwable? = null
    try {
        val session = guest.session(expectedStopGeneration = expectedStopGeneration)
        check(session.agentDirectory == agentDirectory) { "容器用户已变化，请重新查找" }
        val directory = "$agentDirectory/wawlink/content-search"
        session.mkdirs(directory)
        for (name in listOf("container-history-search.mjs", "history-search.mjs", "attachment-prompts.mjs", "diagnostics.mjs")) {
            val source = requireNotNull(OmpProcessPool::class.java.getResourceAsStream("/omp-management/$name")).bufferedReader().use { it.readText() }
            val path = "$directory/$name"
            if (session.readText(path) != source) session.writeText(path, source, expectedVersion = session.version(path))
        }
        val requestContext = currentCoroutineContext()
        withContext(NonCancellable) {
            withContainerFileOperation(runtime, containerId, session.stopGeneration, runningOnly = true, expectedRunGeneration = session.runGeneration) {
                requestContext.ensureActive()
                runtime.openRunningProcess(containerId, listOf("/usr/local/bin/bun", "$directory/container-history-search.mjs"), "/",
                    mapOf("WAWLINK_HISTORY_AGENT_DIR" to agentDirectory, "PI_NO_PTY" to "1", "PI_NO_TITLE" to "1")).also { process = it }
            }
        }
        requestContext.ensureActive()
        val child = requireNotNull(process)
        stderr = launch(Dispatchers.IO) {
            try { child.errorStream.use { input -> val bytes = ByteArray(4096); while (input.read(bytes) >= 0) { } } }
            catch (_: IOException) { }
        }
        val targetIds = ids.toHashSet()
        val boundFiles = targets.associate { it["id"]!!.jsonPrimitive.content to it["sessionFile"]?.jsonPrimitive?.contentOrNull }
        val reading = async(Dispatchers.IO) {
            var complete = false
            readManagementFrames(child.inputStream, maxCharacters = 64 * 1024) { frame ->
                ensureActive()
                if (!session.isCurrent()) throw OmpProcessStoppedException()
                fun text(key: String): String = frame[key]?.jsonPrimitive?.contentOrNull.orEmpty()
                val update = when (text("type")) {
                    "match" -> {
                        val excerpt = text("text")
                        val start = frame["matchStart"]?.jsonPrimitive?.intOrNull ?: -1
                        val end = frame["matchEnd"]?.jsonPrimitive?.intOrNull ?: -1
                        val appId = text("appSessionId")
                        check(appId in targetIds && start >= 0 && end > start && end <= excerpt.length && text("nativeSessionId").isNotBlank() && text("entryId").isNotBlank()) { "全文查找返回了无效的消息身份" }
                        val key = MessageDigest.getInstance("SHA-256").digest(appId.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
                        val file = text("sessionFile")
                        check(file.startsWith("$agentDirectory/wawlink/sessions/$key/") && file.substringAfter("$agentDirectory/wawlink/sessions/$key/").let { !it.contains('/') && it.endsWith(".jsonl") && it != ".jsonl" }) { "全文查找返回了越界历史路径" }
                        check(boundFiles[appId]?.let { it == file } != false) { "全文查找返回了其他历史文件" }
                        ContainerHistorySearchUpdate.Match(ContainerHistorySearchMatch(appId, text("nativeSessionId"), text("entryId"), file,
                            text("role"), text("entryType"), excerpt, start, end))
                    }
                    "progress" -> {
                        val completed = frame["completed"]?.jsonPrimitive?.intOrNull ?: -1
                        val total = frame["total"]?.jsonPrimitive?.intOrNull ?: -1
                        check(total == ids.size && completed in 0..total) { "全文查找进度无效" }
                        ContainerHistorySearchUpdate.Progress(completed, total)
                    }
                    "failure" -> {
                        check(text("appSessionId") in targetIds) { "全文查找返回了无效的会话范围" }
                        ContainerHistorySearchUpdate.Failure(text("appSessionId"), text("message"))
                    }
                    "complete" -> { complete = true; ContainerHistorySearchUpdate.Complete }
                    "error" -> throw IOException(text("message").ifBlank { "全文查找进程失败" })
                    else -> throw IOException("全文查找响应无效")
                }
                trySendBlocking(update).getOrThrow()
            }
            check(complete) { "全文查找进程提前退出，请重试" }
        }
        reader = reading
        withContext(Dispatchers.IO) {
            child.outputStream.bufferedWriter(Charsets.UTF_8).use { output ->
                output.write(buildJsonObject { put("agentDirectory", agentDirectory); put("targets", JsonArray(targets)); put("query", value) }.toString())
            }
        }
        reading.await()
    } catch (caught: Throwable) {
        failure = caught
        throw caught
    } finally {
        withContext(NonCancellable + Dispatchers.IO) {
            var cleanupFailure: Throwable? = null
            process?.let { child ->
                runCatching { child.outputStream.close() }
                try { runtime.closeProcess(containerId, child) } catch (caught: Throwable) { cleanupFailure = caught }
                runCatching { child.inputStream.close() }; runCatching { child.errorStream.close() }
            }
            reader?.cancel(); stderr?.cancel(); guest.close()
            cleanupFailure?.let { cleanup -> if (failure != null) failure?.addSuppressed(cleanup) else throw cleanup }
        }
    }
}
