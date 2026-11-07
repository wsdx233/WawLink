package dev.wawlink.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import dev.wawlink.protocol.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Recovery stays available even after the error is dismissed or another turn succeeds. */
@Composable
internal fun FailedPromptRecovery(controller: AgentSessionController, failed: List<FailedPrompt>, draft: ComposerDraftState) {
    if (failed.isEmpty()) return
    val scope = rememberCoroutineScope()
    var selectedId by rememberSaveable { mutableStateOf<String?>(null) }
    val selected = failed.firstOrNull { it.id == selectedId } ?: failed.last()
    var menu by remember { mutableStateOf(false) }
    var replacing by remember { mutableStateOf<String?>(null) }
    var busy by remember { mutableStateOf(false) }
    var error by remember { mutableStateOf<String?>(null) }
    fun recover(id: String, retry: Boolean) {
        if (busy || draft.takingQueuedPrompt) return
        busy = true
        if (!retry) draft.takingQueuedPrompt = true
        scope.launch {
            try {
                // Once taken, restoring into the composer must finish even during a page transition.
                withContext(NonCancellable + Dispatchers.Main.immediate) {
                    if (retry) withContext(Dispatchers.IO) { controller.retryFailedPrompt(id) }
                    else {
                        val prompt = withContext(Dispatchers.IO) { controller.takeFailedPrompt(id) }
                        draft.takingQueuedPrompt = false
                        draft.restore(prompt, replaceExisting = true)
                    }
                }
                error = null
            } catch (failure: Exception) {
                error = failure.message ?: "无法取回发送内容"
            } finally {
                draft.takingQueuedPrompt = false
                busy = false
            }
        }
    }
    Column(Modifier.fillMaxWidth().padding(horizontal = 16.dp)) {
        error?.let { ErrorMessage(it, fontSize = 17.sp) }
        Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(4.dp)) {
            Box(Modifier.weight(1f)) {
                TextButton({ menu = true }, enabled = failed.size > 1 && !busy) {
                    Text(if (failed.size > 1) "待处理 ${failed.size} 条" else if (selected.retryable) "未发送" else "待确认", fontSize = 17.sp)
                    if (failed.size > 1) Symbol("arrow_drop_down", size = 20.dp)
                }
                DropdownMenu(menu, { menu = false }) {
                    failed.forEach { prompt ->
                        DropdownMenuItem(text = { Text(prompt.text.ifBlank { prompt.attachments.joinToString(" ") { it.name } }, maxLines = 2, overflow = TextOverflow.Ellipsis, fontSize = 17.sp) },
                            onClick = { selectedId = prompt.id; menu = false },
                            leadingIcon = { Symbol("draft") }, trailingIcon = { if (prompt.id == selected.id) Symbol("check") })
                    }
                }
            }
            if (busy) CircularProgressIndicator(Modifier.padding(12.dp).size(24.dp), strokeWidth = 2.dp)
            else {
                TextButton({ if (draft.hasContent || !selected.retryable) replacing = selected.id else recover(selected.id, false) }, enabled = !draft.takingQueuedPrompt) {
                    Symbol("restore", size = 20.dp); Spacer(Modifier.width(6.dp)); Text("取回", fontSize = 17.sp)
                }
                TextButton({ recover(selected.id, true) }, enabled = !draft.takingQueuedPrompt && selected.retryable) {
                    Symbol("refresh", size = 20.dp); Spacer(Modifier.width(6.dp)); Text("重发", fontSize = 17.sp)
                }
            }
        }
    }
    replacing?.let { id ->
        val uncertain = failed.firstOrNull { it.id == id }?.retryable == false
        AlertDialog(onDismissRequest = { replacing = null }, title = { Text(if (draft.hasContent) "替换当前草稿？" else "取回发送内容？") },
            text = { Text(if (uncertain) "发送结果尚未确认，内容可能已被接收。取回后请先核对历史，避免重复发送。" + if (draft.hasContent) "当前草稿将被替换。" else ""
                else "将用未发送的原文和附件替换输入框内容。", fontSize = 18.sp) },
            confirmButton = { TextButton({ replacing = null; recover(id, false) }) { Text(if (draft.hasContent) "替换" else "取回", fontSize = 17.sp) } },
            dismissButton = { TextButton({ replacing = null }) { Text("取消", fontSize = 17.sp) } })
    }
}
