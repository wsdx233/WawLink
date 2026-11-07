package dev.wawlink.app

import androidx.compose.animation.core.tween
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.text.SpanStyle
import androidx.compose.ui.text.buildAnnotatedString
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import dev.wawlink.driver.ContainerHistorySearchMatch
import dev.wawlink.driver.ContainerHistorySearchUpdate
import dev.wawlink.driver.searchSessionContents
import dev.wawlink.ui.*
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Files
import kotlinx.coroutines.*
import kotlinx.serialization.json.*

/** Search starts only through a user action and never restores unselected chat controllers. */
@Composable
internal fun ContainerHistorySearchScreen(
    app: AppCoordinator,
    containerId: String,
    onBack: () -> Unit,
    onOpen: (ContainerHistorySearchMatch) -> Unit,
) {
    val sessions by app.sessions.collectAsState()
    val containers by app.runtime.containers.collectAsState()
    val container = containers.firstOrNull { it.config.id == containerId }?.config
    val records = sessions.filter { it.record.containerId == containerId && !app.isEmptyDraft(it) }.associate { it.record.id to it.record }
    var query by rememberSaveable(containerId) { mutableStateOf("") }
    val matches = remember(containerId) { mutableStateListOf<ContainerHistorySearchMatch>() }
    val failures = remember(containerId) { mutableStateListOf<ContainerHistorySearchUpdate.Failure>() }
    var error by remember { mutableStateOf<String?>(null) }
    var searching by remember { mutableStateOf(false) }
    var searched by remember { mutableStateOf(false) }
    var cancelled by remember { mutableStateOf(false) }
    var completed by remember { mutableIntStateOf(0) }
    var total by remember { mutableIntStateOf(0) }
    var found by remember { mutableIntStateOf(0) }
    var page by remember { mutableIntStateOf(0) }
    var pageLoading by remember { mutableStateOf(false) }
    var store by remember { mutableStateOf<ContainerHistorySearchResults?>(null) }
    var generation by remember { mutableIntStateOf(0) }
    var job by remember { mutableStateOf<Job?>(null) }
    val scope = rememberCoroutineScope()
    fun discardResults() {
        val previous = store
        store = null
        if (previous != null) CoroutineScope(Dispatchers.IO).launch { closeHistorySearchResults(app, previous) }
        found = 0; page = 0; pageLoading = false; matches.clear()
    }
    fun stop() { generation++; job?.cancel(); job = null; searching = false; pageLoading = false }
    fun edit(value: String) {
        stop(); query = value; searched = false; cancelled = false
        discardResults(); failures.clear(); error = null
    }
    fun search() {
        if (container == null || query.isBlank()) return
        stop()
        val requestGeneration = generation
        val ids = records.keys.toList()
        searching = true; searched = true; cancelled = false; error = null
        completed = 0; total = ids.size; discardResults(); failures.clear()
        job = scope.launch {
            var destination: ContainerHistorySearchResults? = null
            var primaryFailure: Throwable? = null
            try {
                val created = withContext(NonCancellable + Dispatchers.IO) {
                    ContainerHistorySearchResults.create(app.dataDir).also { destination = it }
                }
                currentCoroutineContext().ensureActive()
                if (generation != requestGeneration) return@launch
                store = created
                app.pool.searchSessionContents(containerId, ids, query).collect { update ->
                    if (generation != requestGeneration) return@collect
                    when (update) {
                        is ContainerHistorySearchUpdate.Match -> {
                            val index = withContext(Dispatchers.IO) { created.append(update.value) }
                            if (generation == requestGeneration) {
                                found = index + 1
                                if (index / HISTORY_SEARCH_PAGE_SIZE == page) matches.add(update.value)
                            }
                        }
                        is ContainerHistorySearchUpdate.Progress -> { completed = update.completed; total = update.total }
                        is ContainerHistorySearchUpdate.Failure -> failures.add(update)
                        ContainerHistorySearchUpdate.Complete -> searching = false
                    }
                }
            } catch (cancelled: CancellationException) {
                primaryFailure = cancelled
                throw cancelled
            } catch (failure: Exception) {
                primaryFailure = failure
                if (generation == requestGeneration) error = failure.message ?: "全文搜索失败"
            } finally {
                if (generation == requestGeneration) searching = false
                if (store !== destination) withContext(NonCancellable + Dispatchers.IO) { destination?.let { closeHistorySearchResults(app, it, primaryFailure) } }
            }
        }
    }
    fun changePage(next: Int) {
        val source = store ?: return
        if (pageLoading || next < 0 || next * HISTORY_SEARCH_PAGE_SIZE >= found) return
        if (searching && (next + 1) * HISTORY_SEARCH_PAGE_SIZE > found) return
        val requestGeneration = generation
        pageLoading = true
        scope.launch {
            try {
                val rows = withContext(Dispatchers.IO) { source.readPage(next) }
                if (requestGeneration == generation && store === source) { page = next; matches.clear(); matches.addAll(rows) }
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (failure: Exception) { if (requestGeneration == generation) error = failure.message ?: "搜索结果读取失败" }
            finally { if (requestGeneration == generation) pageLoading = false }
        }
    }
    DisposableEffect(containerId) {
        onDispose {
            generation++; job?.cancel()
            val previous = store
            store = null
            if (previous != null) CoroutineScope(Dispatchers.IO).launch { closeHistorySearchResults(app, previous) }
        }
    }
    Column(Modifier.fillMaxSize()) {
        AgentTopBar("会话全文搜索", onBack, actions = {
            if (searching) AgentIconButton("close", "取消查找", { stop(); cancelled = true })
        })
        if (container == null) {
            EmptyHost("容器已不存在")
        } else {
            Column(Modifier.fillMaxWidth().padding(horizontal = 20.dp).padding(top = 8.dp, bottom = 16.dp), verticalArrangement = Arrangement.spacedBy(16.dp)) {
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    ContainerAvatar(container, size = 36.dp)
                    Text(container.displayName, fontSize = 20.sp, maxLines = 1, overflow = TextOverflow.Ellipsis)
                }
                OutlinedTextField(query, ::edit, Modifier.fillMaxWidth(), placeholder = { Text("查找正文或附件名", fontSize = 18.sp) },
                    leadingIcon = { Symbol("search") }, trailingIcon = { if (query.isNotEmpty()) AgentIconButton("close", "清除搜索", { edit("") }) },
                    singleLine = true, shape = RoundedCornerShape(28.dp), textStyle = MaterialTheme.typography.bodyLarge.copy(fontSize = 18.sp))
                HostAction("搜索", "manage_search", ::search, Modifier.fillMaxWidth(), enabled = query.isNotBlank() && !searching)
            }
            if (searching) {
                if (total > 0) LinearProgressIndicator(progress = { completed.toFloat() / total }, modifier = Modifier.fillMaxWidth())
                else LinearProgressIndicator(Modifier.fillMaxWidth())
                Text("已查找 $completed 个会话，共 $total 个", Modifier.padding(horizontal = 20.dp, vertical = 12.dp), fontSize = 17.sp)
            }
            error?.let { ErrorMessage(it, Modifier.padding(horizontal = 20.dp, vertical = 8.dp)) }
            if (cancelled) Text("查找已取消", Modifier.padding(horizontal = 20.dp, vertical = 8.dp), fontSize = 18.sp)
            if (found > 0 || searched && !searching && error == null && !cancelled) {
                Text(if (searching || cancelled || error != null || failures.isNotEmpty()) "已找到 $found 条消息" else "找到 $found 条消息",
                    Modifier.padding(horizontal = 20.dp, vertical = 12.dp), fontSize = 18.sp, color = MaterialTheme.colorScheme.primary)
            }
            if (pageLoading) LinearProgressIndicator(Modifier.fillMaxWidth())
            LazyColumn(Modifier.weight(1f).fillMaxWidth(), contentPadding = PaddingValues(start = 20.dp, end = 20.dp, bottom = 24.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                items(failures, key = { "failure:${it.appSessionId}" }) { failure ->
                    Column(Modifier.fillMaxWidth().padding(vertical = 8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        Text(records[failure.appSessionId]?.title ?: "会话已移除", fontSize = 18.sp)
                        ErrorMessage(failure.message)
                    }
                }
                items(matches, key = { "${it.appSessionId}:${it.nativeSessionId}:${it.entryId}" }) { match ->
                    val highlight = MaterialTheme.colorScheme.primaryContainer
                    val text = remember(match, highlight) { buildAnnotatedString {
                        append(match.text); addStyle(SpanStyle(background = highlight), match.matchStart, match.matchEnd)
                    } }
                    Row(Modifier.fillMaxWidth().animateItem(fadeInSpec = tween(320, easing = EmphDecelEasing), placementSpec = null, fadeOutSpec = tween(180))
                        .clip(RoundedCornerShape(20.dp)).background(MaterialTheme.colorScheme.surfaceContainerLow).clickable { onOpen(match) }.padding(16.dp), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                        Symbol(when (match.role) { "user" -> "person"; "assistant" -> "smart_toy"; "toolResult" -> "build"; else -> "description" }, tint = MaterialTheme.colorScheme.primary)
                        Column(Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                            Text(records[match.appSessionId]?.title ?: "会话已移除", fontSize = 18.sp, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            Text(text, fontSize = 17.sp, lineHeight = 25.sp, maxLines = 5, overflow = TextOverflow.Ellipsis)
                        }
                        Symbol("chevron_right", contentDescription = "打开消息")
                    }
                }
            }
            if (found > HISTORY_SEARCH_PAGE_SIZE) Row(Modifier.fillMaxWidth().padding(horizontal = 20.dp, vertical = 12.dp), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.SpaceBetween) {
                IconButton({ changePage(page - 1) }, enabled = page > 0 && !pageLoading) { Symbol("chevron_left", contentDescription = "上一页") }
                Text("第 ${page + 1} 页", fontSize = 18.sp)
                IconButton({ changePage(page + 1) }, enabled = !pageLoading && if (searching) (page + 2) * HISTORY_SEARCH_PAGE_SIZE <= found else (page + 1) * HISTORY_SEARCH_PAGE_SIZE < found) { Symbol("chevron_right", contentDescription = "下一页") }
            }
        }
    }
}

private const val HISTORY_SEARCH_PAGE_SIZE = 64

/** Disposal failures are reported through the existing diagnostic UI, never an uncaught IO job. */
private fun closeHistorySearchResults(app: AppCoordinator, results: ContainerHistorySearchResults, primaryFailure: Throwable? = null) {
    try { results.close() }
    catch (cleanup: Exception) {
        primaryFailure?.addSuppressed(cleanup)
        app.showError(IllegalStateException("清理搜索缓存失败", cleanup))
    }
}

/** Private temporary excerpts, with one offset per page; the UI holds only the selected page. */
private class ContainerHistorySearchResults private constructor(private val file: File) : AutoCloseable {
    private val output = RandomAccessFile(file, "rw")
    private val offsets = mutableListOf<Long>()
    private var count = 0
    private var closed = false
    @Synchronized fun append(match: ContainerHistorySearchMatch): Int {
        check(!closed) { "搜索结果已关闭" }
        output.seek(output.length())
        if (count % HISTORY_SEARCH_PAGE_SIZE == 0) offsets.add(output.filePointer)
        val bytes = buildJsonObject {
            put("appSessionId", match.appSessionId); put("nativeSessionId", match.nativeSessionId); put("entryId", match.entryId)
            put("sessionFile", match.sessionFile); put("role", match.role); put("entryType", match.entryType)
            put("text", match.text); put("matchStart", match.matchStart); put("matchEnd", match.matchEnd)
        }.toString().toByteArray(Charsets.UTF_8)
        output.writeInt(bytes.size); output.write(bytes)
        return count++
    }
    @Synchronized fun readPage(page: Int): List<ContainerHistorySearchMatch> {
        check(!closed) { "搜索结果已关闭" }
        val offset = offsets.getOrNull(page) ?: return emptyList()
        output.seek(offset)
        return List(minOf(HISTORY_SEARCH_PAGE_SIZE, count - page * HISTORY_SEARCH_PAGE_SIZE)) {
            val length = output.readInt()
            check(length in 1..65536) { "搜索结果损坏" }
            val bytes = ByteArray(length); output.readFully(bytes)
            val value = Json.parseToJsonElement(bytes.toString(Charsets.UTF_8)).jsonObject
            fun text(key: String) = value.getValue(key).jsonPrimitive.content
            ContainerHistorySearchMatch(text("appSessionId"), text("nativeSessionId"), text("entryId"), text("sessionFile"), text("role"),
                text("entryType"), text("text"), value.getValue("matchStart").jsonPrimitive.int, value.getValue("matchEnd").jsonPrimitive.int)
        }
    }
    @Synchronized override fun close() {
        if (closed) return
        closed = true
        var failure: Exception? = null
        try { output.close() } catch (caught: Exception) { failure = caught }
        try {
            synchronized(live) {
                live.remove(file.absolutePath)
                check(!file.exists() || file.delete()) { "无法删除搜索缓存文件" }
            }
        } catch (caught: Exception) {
            if (failure != null) failure.addSuppressed(caught) else failure = caught
        }
        failure?.let { throw it }
    }
    companion object {
        private val live = mutableSetOf<String>()
        fun create(dataDir: File): ContainerHistorySearchResults = synchronized(live) {
            val directory = File(dataDir, "cache/history-search")
            check(directory.isDirectory || directory.mkdirs()) { "无法创建搜索缓存目录" }
            directory.listFiles()?.filter { it.name.startsWith("content-") && it.name.endsWith(".tmp") && it.absolutePath !in live }?.forEach { it.delete() }
            val file = Files.createTempFile(directory.toPath(), "content-", ".tmp").toFile()
            try {
                check(file.setReadable(false, false) && file.setWritable(false, false) && file.setReadable(true, true) && file.setWritable(true, true)) { "无法保护搜索缓存" }
                ContainerHistorySearchResults(file).also { live.add(file.absolutePath) }
            } catch (failure: Throwable) { file.delete(); throw failure }
        }
    }
}
