package dev.wawlink.app

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ListItem
import androidx.compose.material3.ListItemDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.key
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import dev.wawlink.driver.OmpInstallation
import dev.wawlink.driver.OmpInstallationCheck
import dev.wawlink.driver.OmpInstallationState
import dev.wawlink.protocol.AgentManagementState
import dev.wawlink.protocol.ManagedModel
import dev.wawlink.runtime.withContainerFileOperation
import dev.wawlink.runtime.wasInterrupted
import dev.wawlink.runtime.DeploymentProgress
import dev.wawlink.ui.AgentTopBar
import dev.wawlink.ui.ErrorMessage
import dev.wawlink.ui.Symbol
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout

internal data class NativeModelReadiness(
    val model: ManagedModel? = null,
    val status: String,
    val error: String? = null,
    val configureDefault: Boolean = false,
)

/** The native role's exact identity wins; catalogue ordering never selects a replacement. */
internal fun nativeModelReadiness(state: AgentManagementState): NativeModelReadiness {
    if (state.loading || state.modelsLoading) return NativeModelReadiness(status = "正在读取模型与认证")
    (state.error ?: state.modelsError)?.let { return NativeModelReadiness(status = "模型配置读取失败", error = it) }
    val available = state.models.filter { it.kind == "chat" && it.available }
    if (available.isEmpty()) return NativeModelReadiness(status = "请配置模型服务或认证")
    val role = state.roles.firstOrNull { it.id == "default" }
        ?: return NativeModelReadiness(status = "未能读取默认模型", error = "原生响应缺少默认模型角色", configureDefault = true)
    role.error?.let { return NativeModelReadiness(status = "默认模型不可用", error = it, configureDefault = true) }
    val provider = role.providerId?.takeIf { it.isNotBlank() }
    val modelId = role.modelId?.takeIf { it.isNotBlank() }
    if (provider == null || modelId == null) return NativeModelReadiness(status = "默认模型未指定", configureDefault = true)
    val model = available.singleOrNull { it.provider == provider && it.id == modelId }
        ?: return NativeModelReadiness(status = "默认模型当前不可用", configureDefault = true)
    return NativeModelReadiness(model, model.name.ifBlank { model.id })
}

/** Entering this page is the explicit request to check the selected container only. */
@Composable
internal fun SessionReadinessScreen(
    app: AppCoordinator,
    onBack: () -> Unit,
    onInstallContainer: () -> Unit,
    onConfigureModels: (String) -> Unit,
    onConfigureAgent: (String) -> Unit,
    onRequestOmpInstall: (String, OmpInstallationCheck) -> Unit,
    onStartSession: (String) -> Unit,
) {
    val containers by app.runtime.containers.collectAsState()
    val deployments by app.runtime.deployments.collectAsState()
    val selectedId by app.selectedContainerId.collectAsState()
    val container = containers.firstOrNull { it.config.id == selectedId }
    val id = container?.config?.id
    val preparingContainer = id == null && deployments.any { it.error == null }
    var refresh by remember { mutableStateOf(0) }
    var phase by remember(id) { mutableStateOf<String?>(if (id == null) null else "检查 OMP") }
    var installation by remember(id) { mutableStateOf<OmpInstallationCheck?>(null) }
    var model by remember(id) { mutableStateOf<NativeModelReadiness?>(null) }
    var failure by remember(id) { mutableStateOf<String?>(null) }
    var interrupted by remember(id) { mutableStateOf(false) }
    var validatedStop by remember(id) { mutableStateOf<Long?>(null) }
    var validatedRun by remember(id) { mutableStateOf<Long?>(null) }
    val checking = phase != null
    val installed = installation?.state == OmpInstallationState.READY
    val current = container != null && container.running && validatedStop == container.stopGeneration && validatedRun == container.runGeneration
    val ready = !checking && current && installed && model?.model != null && failure == null

    LaunchedEffect(id, refresh) {
        if (id == null) return@LaunchedEffect
        val initial = app.runtime.containers.value.firstOrNull { it.config.id == id } ?: return@LaunchedEffect
        installation = null
        model = null
        failure = null
        interrupted = false
        validatedStop = null
        validatedRun = null
        phase = "检查 OMP"
        try {
            withTimeout(150_000) {
                withContainerFileOperation(app.runtime, id, expectedStopGeneration = initial.stopGeneration) {
                    val check = OmpInstallation.inspect(app.runtime, id, stopGeneration = initial.stopGeneration)
                    currentCoroutineContext().ensureActive()
                    installation = check
                    val inspected = app.runtime.containers.value.firstOrNull { it.config.id == id }
                        ?: throw CancellationException("容器已移除")
                    if (!inspected.running || inspected.stopGeneration != initial.stopGeneration) throw CancellationException("容器已停止或重新启动")
                    validatedStop = inspected.stopGeneration
                    validatedRun = inspected.runGeneration
                    if (check.state == OmpInstallationState.READY) {
                        phase = "读取模型与认证"
                        // No eager background load: the explicit reload inherits the operation's
                        // stop/run identity and cannot reopen a manually stopped container.
                        val repository = app.pool.management(id, "/workspace", loadInitially = false)
                        repository.reload()
                        val native = repository.state.first { !it.loading && !it.modelsLoading }
                        currentCoroutineContext().ensureActive()
                        model = nativeModelReadiness(native)
                    }
                }
            }
        } catch (timeout: TimeoutCancellationException) {
            currentCoroutineContext().ensureActive()
            failure = "${phase ?: "就绪检查"}超时\n总期限 150 秒\n${timeout.message.orEmpty()}"
        } catch (cancelled: CancellationException) {
            currentCoroutineContext().ensureActive()
            interrupted = true
            installation = null
            model = null
        } catch (error: Exception) {
            if (app.runtime.wasInterrupted(id, initial.stopGeneration)) {
                interrupted = true
                installation = null
                model = null
            } else failure = "${phase ?: "就绪检查"}失败\n${hostErrorDetails(error)}"
        } finally {
            phase = null
        }
    }
    LaunchedEffect(id, container?.stopGeneration, container?.runGeneration, container?.running) {
        if (validatedStop != null && !current) {
            model = null
            installation = null
            interrupted = true
        }
    }

    Column(Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface)) {
        AgentTopBar("开始使用", onBack)
        Column(
            Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(horizontal = 16.dp, vertical = 12.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            ReadinessRow("运行容器", container?.config?.displayName ?: if (preparingContainer) "正在准备容器" else "添加一个 Linux 容器", "dns", ready = container != null,
                onClick = if (container == null && !preparingContainer) onInstallContainer else null)
            if (deployments.isNotEmpty()) ReadinessDeployments(app, deployments)
            ReadinessRow("OMP", when {
                id == null -> "等待容器"
                interrupted -> "容器状态已改变，请重新检查"
                phase == "检查 OMP" -> "正在检查"
                installation?.state == OmpInstallationState.READY -> "已就绪"
                installation?.state == OmpInstallationState.MISSING -> "尚未安装"
                installation?.state == OmpInstallationState.BROKEN -> "需要修复"
                else -> "等待检查"
            }, "smart_toy", ready = installed, checking = phase == "检查 OMP")
            ReadinessRow("默认模型", when {
                phase == "读取模型与认证" -> "正在读取模型与认证"
                !installed -> "等待 OMP"
                else -> model?.status ?: "等待检查"
            }, "deployed_code", ready = model?.model != null && current, checking = phase == "读取模型与认证")
            ReadinessRow("开始会话", if (ready) "已就绪" else "完成以上准备后开始", "forum", ready = ready)
            failure?.let { ErrorMessage("就绪检查未完成", Modifier.fillMaxWidth(), fontSize = 17.sp, details = it) }
            if (installation?.state == OmpInstallationState.BROKEN) {
                ErrorMessage("OMP 需要修复", Modifier.fillMaxWidth(), fontSize = 17.sp, details = installation!!.message)
            }
            model?.error?.let { ErrorMessage(model!!.status, Modifier.fillMaxWidth(), fontSize = 17.sp, details = it) }
            when {
                id == null -> if (!preparingContainer) HostAction("添加容器", "add", onInstallContainer, Modifier.fillMaxWidth())
                checking -> Unit
                failure != null || interrupted -> HostAction("重新检查", "refresh", { refresh++ }, Modifier.fillMaxWidth())
                installation?.state == OmpInstallationState.MISSING || installation?.state == OmpInstallationState.BROKEN -> {
                    HostAction(if (installation?.state == OmpInstallationState.MISSING) "安装 OMP" else "修复 OMP", "download",
                        { installation?.let { onRequestOmpInstall(id, it) } }, Modifier.fillMaxWidth())
                }
                model?.model == null -> HostAction(if (model?.configureDefault == true) "设置默认模型" else "配置模型服务", "deployed_code",
                    { if (model?.configureDefault == true) onConfigureAgent(id) else onConfigureModels(id) }, Modifier.fillMaxWidth())
                else -> HostAction("开始会话", "add", { if (ready) onStartSession(id) }, Modifier.fillMaxWidth(), enabled = ready)
            }
            if (id != null && !checking && failure == null && !interrupted) {
                TextButton({ refresh++ }, Modifier.fillMaxWidth()) { Symbol("refresh"); Text("重新检查", Modifier.padding(start = 8.dp), fontSize = 17.sp) }
            }
        }
    }
}

@Composable
private fun ReadinessDeployments(app: AppCoordinator, tasks: List<DeploymentProgress>) {
    tasks.forEach { task -> key(task.id) {
        var busy by remember { mutableStateOf(false) }
        fun perform(work: suspend () -> Unit) {
            if (busy) return
            busy = true
            app.launch { try { work() } finally { busy = false } }
        }
        Column(Modifier.fillMaxWidth().padding(horizontal = 16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(task.name, fontSize = 20.sp)
            ContainerOperationProgress(task)
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.End) {
                if (task.cancellable) TextButton({ perform { app.runtime.cancelDeployment(task.id) } }, enabled = !busy) {
                    Symbol("close", size = 20.dp); Spacer(Modifier.width(8.dp)); Text("取消", fontSize = 17.sp)
                }
                if (task.retryable) {
                    TextButton({ perform { app.runtime.dismissDeployment(task.id) } }, enabled = !busy) {
                        Symbol("clear_all", size = 20.dp); Spacer(Modifier.width(8.dp)); Text("清除", fontSize = 17.sp)
                    }
                    TextButton({ perform { app.retryDistroDeployment(task.id) } }, enabled = !busy) {
                        Symbol("refresh", size = 20.dp); Spacer(Modifier.width(8.dp)); Text("重试", fontSize = 17.sp)
                    }
                }
            }
        }
    } }
}

@Composable
private fun ReadinessRow(title: String, status: String, icon: String, ready: Boolean, checking: Boolean = false, onClick: (() -> Unit)? = null) {
    ListItem(
        headlineContent = { Text(title, fontSize = 20.sp) },
        supportingContent = { Text(status, fontSize = 17.sp) },
        leadingContent = { Symbol(icon, size = 26.dp) },
        trailingContent = {
            when {
                checking -> CircularProgressIndicator(Modifier.size(26.dp), strokeWidth = 2.dp)
                ready -> Symbol("check_circle", tint = MaterialTheme.colorScheme.primary, contentDescription = "已就绪")
                onClick != null -> Symbol("chevron_right")
            }
        },
        colors = ListItemDefaults.colors(containerColor = MaterialTheme.colorScheme.surface),
        modifier = Modifier.fillMaxWidth().then(if (onClick == null) Modifier else Modifier.clickable(onClick = onClick)),
    )
}
