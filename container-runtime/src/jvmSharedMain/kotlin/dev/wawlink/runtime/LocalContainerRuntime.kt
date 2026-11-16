package dev.wawlink.runtime

import java.io.File
import java.io.FileOutputStream
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/** Script and shell are positional parameters, never interpolated into shell source or pasted into the PTY. */
internal fun terminalArguments(shell: String, initialCommand: String?): List<String> {
    if (initialCommand == null) return listOf(shell, "-i")
    require('\u0000' !in initialCommand) { "终端初始命令不能包含 NUL" }
    return listOf("/bin/sh", "-c", """
        trap ':' INT
        /bin/sh -c "${'$'}1"
        status=${'$'}?
        trap - INT
        if [ "${'$'}status" -ne 0 ]; then printf '\n初始命令失败（退出码 %s）；终端保持打开。\n' "${'$'}status"; fi
        exec "${'$'}2" -i
    """.trimIndent(), "wawlink-terminal", initialCommand, shell)
}

@Serializable
private data class SavedRuntime(
    val containers: List<ContainerConfig> = emptyList(),
    val debugRootfsRemoved: Boolean = false,
    val sizes: Map<String, Long> = emptyMap(),
)

/** One application-owned runtime. All guest commands go through the selected engine. */
class LocalContainerRuntime internal constructor(
    val paths: RuntimePaths,
    private val measureRootfs: (File, Set<String>) -> Long,
    private val startRootProcess: (String) -> Process = { ProcessBuilder("su", "-c", it).start() },
    private val resolveDistro: (DistroSpec, MirrorSource, DistroTransferControl) -> DistroDownload = DistroSources::resolve,
    runRootCommand: ((String) -> String)?,
) : ContainerRuntime {
    constructor(paths: RuntimePaths) : this(paths, RootfsArchives::size)
    internal constructor(paths: RuntimePaths, measureRootfs: (File, Set<String>) -> Long) :
        this(paths, measureRootfs, runRootCommand = null)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mutex = Mutex()
    private val engines = RuntimeEngines(paths, runRootCommand)
    private val metadataStore = ContainerMetadataStore(engines)
    private val json = Json { prettyPrint = true; ignoreUnknownKeys = true }
    private val stateFile = File(paths.dataDir, "containers.json")
    private val rootfsDir = File(paths.dataDir, "rootfs")
    private val cacheDir = File(paths.dataDir, "downloads")
    private val configurations = linkedMapOf<String, ContainerConfig>()
    private val processes = mutableMapOf<String, MutableList<TrackedProcess>>()
    private val keepers = mutableMapOf<String, TrackedProcess>()
    private val runGenerations = mutableMapOf<String, Long>()
    private val stopGenerations = mutableMapOf<String, Long>()
    private var nextRunGeneration = 0L
    private val errors = mutableMapOf<String, String>()
    private val dnsErrors = mutableMapOf<String, String>()
    private val metadataErrors = mutableMapOf<String, String>()
    private val sizes = mutableMapOf<String, Long>()
    private val sizeRequests = mutableMapOf<String, Any>()
    private val rootfsOperations = mutableMapOf<String, RootfsOperation>()
    private class StagedImport(val id: String, val name: String, val directory: File, var owner: Job?) {
        val root = File(directory, "rootfs")
        val archive = File(directory, "archive.part")
        var config: ContainerConfig? = null
        var inspection: RootfsImportInspection? = null
        var discarded = false
    }
    private val rootfsImports = mutableMapOf<String, StagedImport>()
    private class DistroDeployment(val id: String, val distro: DistroSpec, val name: String, val mirror: MirrorSource, val engine: ContainerEngineType) {
        var owner: Job? = null
        var transfer: DistroTransferControl? = null
        var cancelling = false
        var finalizing = false
        var committed = false
        var failed = false
    }
    /** Protected by _deployments along with the corresponding presentation row. */
    private val distroDeployments = mutableMapOf<String, DistroDeployment>()
    private val idleJobs = mutableMapOf<String, Job>()
    private val packageLocks = java.util.concurrent.ConcurrentHashMap<String, Mutex>()
    private var debugRootfsRemoved = false
    @Volatile private var closing = false
    @Volatile private var closed = false
    private val _containers = MutableStateFlow<List<ContainerInfo>>(emptyList())
    override val containers: StateFlow<List<ContainerInfo>> = _containers.asStateFlow()
    private val _deployments = MutableStateFlow<List<DeploymentProgress>>(emptyList())
    override val deployments: StateFlow<List<DeploymentProgress>> = _deployments.asStateFlow()
    override val catalog: List<DistroSpec> = DistroSources.catalog

    init {
        require(paths.dataDir.isDirectory || paths.dataDir.mkdirs()) { "无法创建运行时数据目录" }
        rootfsDir.mkdirs()
        cacheDir.mkdirs()
        if (stateFile.isFile) {
            val saved = try { json.decodeFromString<SavedRuntime>(stateFile.readText()) }
            catch (failure: Exception) { throw IllegalStateException("容器配置文件损坏，已保留原文件：${stateFile.path}", failure) }
            debugRootfsRemoved = saved.debugRootfsRemoved
            saved.containers.forEach { config ->
                require(!configurations.containsKey(config.id)) { "容器配置包含重复 ID：${config.id}" }
                configurations[config.id] = config
            }
            saved.sizes.forEach { (id, size) -> if (id in configurations && size >= 0) sizes[id] = size }
        }
        if (!debugRootfsRemoved && !configurations.containsKey("ubuntu-main")) {
            paths.debugRootfs?.takeIf { it.isDirectory }?.let { root ->
                val config = ContainerConfig("ubuntu-main", "ubuntu-main", root.canonicalPath, isDefault = configurations.values.none { it.isDefault }, link2symlink = paths.isAndroid)
                try { engines.validateRoot(config); configurations[config.id] = config; persist() }
                catch (failure: Exception) { errors[config.id] = "预置 rootfs 无效：${failure.message}" }
            }
        }
        configurations.values.toList().forEach { config ->
            try { engines.validateRoot(config) }
            catch (failure: Exception) { errors[config.id] = "rootfs 不可访问：${failure.message}" }
            refreshMetadata(config)
        }
        publish()
        scope.launch { mutex.withLock { if (!closing && !closed) configurations.values.forEach(::scheduleSize) } }
        paths.resolvConf?.let { resolver ->
            scope.launch {
                resolver.collect { content ->
                    if (content != null) mutex.withLock {
                        for (config in configurations.values) {
                            if (keepers[config.id]?.isAlive != true) continue
                            try { updateDns(config, content) } catch (failure: Exception) {
                                if (failure is CancellationException) throw failure
                                // The affected container exposes the error; keep observing network changes.
                            }
                        }
                    }
                }
            }
        }
    }

    private fun ensureOpen() = check(!closed && !closing) { "容器运行时已关闭" }
    private fun config(id: String): ContainerConfig = configurations[id] ?: error("找不到容器：$id")
    private fun sizeOf(config: ContainerConfig, appOwnedRoot: Boolean = false): Long {
        if (RootfsAccess.requiresRoot(config) && !appOwnedRoot) return RootfsArchives.privilegedSize(config, engines::rootCommand)
        val excluded = RootfsOwnership.exclusions(config).map { it.removePrefix("/") }.toSet()
        return measureRootfs(File(config.rootfsPath).canonicalFile, excluded)
    }

    /** Called during construction or under mutex; only the latest exact configuration may publish. */
    private fun scheduleSize(config: ContainerConfig) {
        val request = Any()
        sizeRequests[config.id] = request
        scope.launch {
            val size = try { sizeOf(config) }
            catch (failure: Exception) {
                currentCoroutineContext().ensureActive()
                return@launch // Keep the last successful cache when an offline tree is inaccessible.
            }
            mutex.withLock {
                if (closed || closing || sizeRequests[config.id] !== request) return@withLock
                val current = configurations[config.id] ?: return@withLock
                if (current !== config) {
                    scheduleSize(current)
                    return@withLock
                }
                sizes[config.id] = size
                try { persist() }
                catch (failure: Exception) { errors.putIfAbsent(config.id, "容器体积缓存保存失败：${failure.message}") }
                publish()
            }
        }
    }

    private fun ensureAvailable(id: String, reservation: String? = null) {
        rootfsOperations[id]?.let { check(it.config.id == reservation) { "容器正在归档，请稍后重试：$id" } }
    }

    private fun ensureDefaultAvailable(id: String) {
        configurations.values.forEach { if (it.isDefault != (it.id == id)) ensureAvailable(it.id) }
    }
    private fun publish() {
        _containers.value = configurations.values.map {
            ContainerInfo(it, keepers[it.id]?.let { keeper -> !keeper.stopping && keeper.isAlive } == true,
                sizes[it.id] ?: 0, errors[it.id] ?: dnsErrors[it.id], runGenerations[it.id] ?: 0,
                metadataErrors[it.id], stopGenerations[it.id] ?: 0)
        }
    }
    private fun persist() {
        val temp = File(paths.dataDir, "containers.json.tmp")
        try {
            FileOutputStream(temp).use { output -> output.write(json.encodeToString(SavedRuntime(configurations.values.toList(), debugRootfsRemoved, sizes)).toByteArray()); output.fd.sync() }
            try { Files.move(temp.toPath(), stateFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING) }
            catch (_: AtomicMoveNotSupportedException) { Files.move(temp.toPath(), stateFile.toPath(), StandardCopyOption.REPLACE_EXISTING) }
        } catch (failure: Throwable) {
            try { Files.deleteIfExists(temp.toPath()) } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            throw failure
        }
    }

    /** Metadata failures are independent of engine/config repair and never start a guest. */
    private fun refreshMetadata(config: ContainerConfig): ContainerMetadataSnapshot? = try {
        metadataStore.read(config).also { snapshot ->
            configurations[config.id] = config.copy(metadata = snapshot.value)
            metadataErrors.remove(config.id)
        }
    } catch (failure: Exception) {
        if (failure is CancellationException) throw failure
        configurations[config.id] = config.copy(metadata = ContainerMetadata())
        metadataErrors[config.id] = "容器信息读取失败：${failure.message}"
        null
    }

    override suspend fun readMetadata(id: String): ContainerMetadataSnapshot = withContext(Dispatchers.IO) { mutex.withLock {
        ensureOpen()
        ensureAvailable(id)
        val snapshot = refreshMetadata(config(id))
        publish()
        snapshot ?: throw IllegalStateException(metadataErrors.getValue(id))
    } }

    override suspend fun saveMetadata(id: String, metadata: ContainerMetadata, expectedRevision: String?): ContainerMetadataSnapshot = withContext(Dispatchers.IO) { mutex.withLock {
        ensureOpen()
        ensureAvailable(id)
        val current = config(id)
        val snapshot = metadataStore.save(current, metadata, expectedRevision)
        configurations[id] = current.copy(metadata = snapshot.value)
        metadataErrors.remove(id)
        publish()
        snapshot
    } }

    private fun updateDns(config: ContainerConfig, content: String) {
        try {
            val overridden = config.mounts.any { mount ->
                val target = mount.target.trimEnd('/')
                target == "/etc" || target == "/etc/resolv.conf"
            }
            if (!overridden) RuntimeDns.update(engines.validateRoot(config), content, config, engines::rootCommand)
            if (dnsErrors.remove(config.id) != null) publish()
        } catch (failure: Exception) {
            if (failure is CancellationException) throw failure
            val message = "DNS 配置更新失败：${failure.message}"
            dnsErrors[config.id] = message
            publish()
            throw IllegalStateException(message, failure)
        }
    }

    override suspend fun register(config: ContainerConfig) { register(config, initialTitle = null) }

    private suspend fun register(config: ContainerConfig, initialTitle: String?, measuredSize: Long? = null, reservation: String? = null, owner: Job? = null, importNames: Pair<String, String>? = null, onProgress: ((String, Float?) -> Unit)? = null): ContainerConfig = withContext(Dispatchers.IO) {
        mutex.withLock {
            currentCoroutineContext().ensureActive()
            if (owner?.isActive == false) throw CancellationException("容器注册已取消")
            ensureOpen()
            ensureAvailable(config.id, reservation)
            require(!configurations.containsKey(config.id)) { "容器 ID 已存在：${config.id}" }
            importNames?.let { (name, title) ->
                require(rootfsImportConflicts(name, title, configurations.values.toList()).isEmpty()) { "容器名称已存在，请选择覆盖更新或重命名" }
            }
            engines.validateRoot(config)
            require(config.id.matches(Regex("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")) && config.name.isNotBlank()) { "容器 ID 或名称无效" }
            val root = File(config.rootfsPath).canonicalFile
            require(configurations.values.none { File(it.rootfsPath).canonicalFile == root }) { "此 rootfs 已注册为另一个容器" }
            require(rootfsOperations.values.none { operation ->
                operation.reservedId != config.id && operation.reservedId?.let { File(rootfsDir, it).canonicalFile == root } == true
            }) { "此 rootfs 正在克隆" }
            if (config.isDefault) ensureDefaultAvailable(config.id)
            val previous = configurations.toMap()
            val report = onProgress ?: progressReporter(config.id, config.displayName)
            try {
                report("保存容器配置", null)
                configurations[config.id] = config.copy(rootfsPath = root.path, isDefault = config.isDefault || configurations.isEmpty())
                if (config.engine == ContainerEngineType.CHROOT && !config.ownershipMigrationDeferred) {
                    val manifest = RootfsOwnership.status(root)
                    if (manifest == null || manifest.physical != ContainerEngineType.CHROOT || manifest.pending) {
                        val context = currentCoroutineContext()
                        RootfsOwnership.migrate(config, config, engines::rootCommand, report) {
                            context.ensureActive()
                            if (owner?.isActive == false) throw CancellationException("容器注册已取消")
                        }
                    }
                }
                RootfsOwnership.requireStable(config)
                report("读取容器信息并保存配置", null)
                val snapshot = refreshMetadata(configurations.getValue(config.id))
                if (initialTitle != null && snapshot != null && snapshot.revision == null) {
                    val saved = metadataStore.save(config, ContainerMetadata(title = initialTitle), null)
                    configurations[config.id] = configurations.getValue(config.id).copy(metadata = saved.value)
                }
                if (config.isDefault) configurations.replaceAll { key, existing ->
                    if (existing.isDefault == (key == config.id)) existing else existing.copy(isDefault = key == config.id)
                }
                if (measuredSize != null) sizes[config.id] = measuredSize
                if (owner?.isActive == false) throw CancellationException("容器注册已取消")
                persist()
            } catch (failure: Throwable) {
                configurations.clear()
                configurations.putAll(previous)
                metadataErrors.remove(config.id)
                sizes.remove(config.id)
                sizeRequests.remove(config.id)
                throw failure
            } finally {
                if (onProgress == null) finishDeployment(config.id)
            }
            val registered = configurations.getValue(config.id)
            if (measuredSize == null) scheduleSize(registered)
            publish()
            registered
        }
    }

    override suspend fun start(id: String) = withContext(Dispatchers.IO) { mutex.withLock { ensureOpen(); startLocked(id) } }
    private suspend fun startLocked(id: String) {
        currentCoroutineContext().ensureActive()
        ensureAvailable(id)
        val config = config(id)
        RootfsOwnership.requireStable(config)
        if (keepers[id]?.let { !it.stopping && it.isAlive } == true) return
        paths.resolvConf?.value?.let { updateDns(config, it) }
        val report = progressReporter(id, config.displayName)
        var prepared = false
        try {
            if (keepers.containsKey(id) || processes[id].orEmpty().isNotEmpty() || engines.hasOwnedMounts(id)) {
                stopLocked(id, intentional = false)
            }
            engines.prepare(config, report)
            prepared = true
            if (config.engine == ContainerEngineType.PROOT || config.engine == ContainerEngineType.PROROOT) {
                RuntimeIdentity.update(engines.validateRoot(config), RuntimeIdentity.hostSupplementaryGroupIds(File("/proc/self/status").readText(Charsets.UTF_8)), config = config)
            }
            val workspace = File(config.rootfsPath, "workspace")
            require(workspace.canonicalFile.toPath().startsWith(File(config.rootfsPath).canonicalFile.toPath())) { "工作区路径越界" }
            workspace.mkdirs()
            report("验证容器引擎启动", null)
            val health = launchLocked(config, listOf("/bin/sh", "-c", "id >/dev/null"), "/", emptyMap(), true, false).process
            val checkOutput = scope.async { health.inputStream.bufferedReader().use { it.readText() } }
            if (!health.waitFor(90, TimeUnit.SECONDS)) {
                error("容器引擎启动超时，请检查 Rootfs/QEMU")
            }
            val output = checkOutput.await()
            check(health.exitValue() == 0) { "容器引擎启动失败：${output.trim()}" }
            currentCoroutineContext().ensureActive()
            report("启动容器服务", null)
            val keeper = launchLocked(config, listOf("/bin/sh", "-c", "trap 'exit 0' TERM INT; while :; do sleep 3600 & wait \$!; done"), "/", emptyMap(), true, true)
            keepers[id] = keeper
            runGenerations[id] = ++nextRunGeneration
            errors.remove(id)
            publish()
        } catch (failure: Throwable) {
            if (prepared) try { stopLocked(id, intentional = false) } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            errors[id] = failure.message ?: "容器启动失败"
            publish()
            throw failure
        } finally {
            finishDeployment(id)
        }
    }

    override suspend fun stop(id: String) = withContext(Dispatchers.IO) { mutex.withLock {
        ensureOpen(); config(id)
        try { stopLocked(id) } catch (failure: Throwable) { errors[id] = failure.message ?: "容器停止失败"; publish(); throw failure }
    } }
    private fun stopLocked(id: String, intentional: Boolean = true) {
        idleJobs.remove(id)?.cancel()
        val owned = processes[id].orEmpty().toList()
        if (intentional) stopGenerations[id] = (stopGenerations[id] ?: 0) + 1
        owned.forEach { it.stopping = true }
        // Readers can observe EOF immediately after TERM; invalidate their lifetime first.
        publish()
        owned.forEach { it.outputDrainer?.cancel(); it.refreshDescendants() }
        val root = configurations[id]?.engine == ContainerEngineType.CHROOT
        terminate(owned, root, false)
        owned.forEach { if (it.process.isAlive) it.process.destroy() }
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3)
        while (System.nanoTime() < deadline && owned.any { it.hasLiveProcesses() }) Thread.sleep(50)
        terminate(owned, root, true)
        owned.forEach { if (it.process.isAlive) it.process.destroyForcibly() }
        waitForTrackedExit(owned, 2)
        if (root && (owned.isNotEmpty() || keepers.containsKey(id) || engines.hasOwnedMounts(id))) {
            val canonicalRoot = RootfsAccess.canonicalRoot(config(id), engines::rootCommand)
            engines.rootCommand(ChrootProcessCleanup.script(canonicalRoot))
        }
        owned.forEach { it.terminal?.close() }
        check(owned.none { it.hasLiveProcesses() }) { "容器仍有已跟踪进程未退出，已保留挂载以保护文件系统" }
        try { engines.unmount(id); errors.remove(id) }
        catch (failure: Throwable) { errors[id] = failure.message.orEmpty(); publish(); throw failure }
        processes.remove(id)
        keepers.remove(id)
        runGenerations.remove(id)
        publish()
    }

    /** KILL delivery is not exit: grandchildren can outlive the held su/engine Process. */
    private fun waitForTrackedExit(owned: List<TrackedProcess>, seconds: Long) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
        while (owned.any { it.hasLiveProcesses() } && System.nanoTime() < deadline) Thread.sleep(50)
    }

    private fun stopIdleLocked(id: String, scheduledStopGeneration: Long?, keeperAtSchedule: TrackedProcess?) {
        if (stopGenerations[id] != scheduledStopGeneration || keepers[id] !== keeperAtSchedule ||
            processes[id].orEmpty().any { !it.keeper && !it.stopping }) return
        try { stopLocked(id) }
        catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { errors[id] = "容器自动休眠失败：${failure.message}"; publish() }
    }

    private fun terminate(owned: List<TrackedProcess>, asRoot: Boolean, force: Boolean) {
        if (!asRoot) owned.forEach { it.terminateFallback(force) }
        val identities = owned.flatMap { it.liveIdentities() }.distinct().asReversed()
        if (identities.isEmpty()) return
        val signal = if (force) "KILL" else "TERM"
        val command = identities.joinToString("\n") { (pid, start) -> """
            stat=${'$'}(cat /proc/$pid/stat 2>/dev/null)
            if [ -n "${'$'}stat" ]; then
                rest=${'$'}{stat##*) }; set -- ${'$'}rest
                if [ "${'$'}#" -ge 20 ]; then
                    shift 19
                    if [ "${'$'}1" = ${shellQuote(start)} ]; then kill -$signal $pid 2>/dev/null || true; fi
                fi
            fi
        """.trimIndent() } + "\ntrue"
        if (asRoot) engines.rootCommand(command) else HostCommands.run(listOf("sh", "-c", command))
    }

    override suspend fun update(config: ContainerConfig, migrateOwnership: Boolean) = withContext(Dispatchers.IO) { trackOperation(config.id, config.displayName, "检查引擎设置") { report ->
        val target = config.copy(ownershipMigrationDeferred = !migrateOwnership)
        val migration = mutex.withLock {
            ensureOpen(); ensureAvailable(config.id)
            if (config.isDefault) ensureDefaultAvailable(config.id)
            val previous = config(config.id)
            require(File(previous.rootfsPath).canonicalPath == File(config.rootfsPath).canonicalPath) { "请通过导入/克隆更换 rootfs，不能直接修改路径" }
            if (!migrateOwnership) RootfsOwnership.requireStable(previous)
            engines.validate(config)
            val manifest = RootfsOwnership.status(File(config.rootfsPath))
            val needed = migrateOwnership && (RootfsOwnership.mapped(previous.engine) != RootfsOwnership.mapped(config.engine) ||
                manifest?.pending == true || (manifest != null && manifest.physical != RootfsOwnership.mapped(config.engine)))
            if (needed) RootfsOwnership.preflight(engines::rootCommand)
            needed
        }
        if (migration) {
            var committedGeneration: Long? = null
            withStoppedRootfs(config.id, restore = false) { previous ->
                revokeHostRootAccess(previous, target)
                val operation = mutex.withLock { rootfsOperations.getValue(config.id) }
                val context = currentCoroutineContext()
                RootfsOwnership.migrate(previous, target, engines::rootCommand, report) {
                    context.ensureActive()
                    check(!closing && !closed && !wasInterrupted(config.id, operation.stopGeneration)) { "容器已停止或关闭；属主迁移已保留，可重新保存目标引擎继续" }
                }
                withContext(NonCancellable) { mutex.withLock {
                    ensureOpen()
                    check(configurations[config.id] === previous && stopGenerations[config.id] == operation.stopGeneration) { "容器操作已过期，未提交目标引擎配置" }
                    commitConfiguration(target, previous)
                    operation.config = configurations.getValue(config.id)
                    committedGeneration = operation.stopGeneration
                } }
                // The lease releases admission before a new engine process is started.
                operation.runGeneration != null
            }.let { wasRunning ->
                if (wasRunning) mutex.withLock {
                    currentCoroutineContext().ensureActive()
                    if (!closing && !closed && configurations[config.id]?.engine == config.engine &&
                        stopGenerations[config.id] == committedGeneration) startLocked(config.id)
                }
            }
        } else mutex.withLock {
            ensureOpen(); ensureAvailable(config.id)
            val previous = config(config.id)
            RootfsOwnership.requireStable(previous)
            val running = keepers[config.id]?.isAlive == true
            if (running || engines.hasOwnedMounts(config.id)) stopLocked(config.id)
            revokeHostRootAccess(previous, target)
            if (!migrateOwnership) RootfsOwnership.retainPhysicalIdentity(previous)
            commitConfiguration(target, previous)
            if (running) startLocked(config.id)
        }
        }
    }

    private fun revokeHostRootAccess(previous: ContainerConfig, next: ContainerConfig) {
        if (previous.engine == ContainerEngineType.CHROOT && previous.hostRootAccess &&
            (next.engine != ContainerEngineType.CHROOT || !next.hostRootAccess)) {
            synchronizeHostExec(previous, engines.validateRoot(previous), false, engines::rootCommand)
        }
    }

    private fun commitConfiguration(config: ContainerConfig, previous: ContainerConfig) {
        val before = configurations.toMap()
        try {
            if (config.isDefault) configurations.replaceAll { _, value -> if (value.isDefault) value.copy(isDefault = false) else value }
            configurations[config.id] = config.copy(metadata = previous.metadata)
            if (configurations.values.none { it.isDefault }) configurations[config.id] = configurations.getValue(config.id).copy(isDefault = true)
            persist()
        } catch (failure: Throwable) {
            configurations.clear(); configurations.putAll(before)
            throw failure
        }
        refreshMetadata(configurations.getValue(config.id))
        scheduleSize(configurations.getValue(config.id))
        publish()
    }

    override suspend fun setDefault(id: String) = withContext(Dispatchers.IO) { mutex.withLock {
        ensureOpen(); config(id)
        ensureAvailable(id); ensureDefaultAvailable(id)
        configurations.replaceAll { key, value -> if (value.isDefault == (key == id)) value else value.copy(isDefault = key == id) }
        persist(); publish()
    } }

    override suspend fun delete(id: String) = withContext(Dispatchers.IO) { mutex.withLock {
        ensureOpen()
        ensureAvailable(id)
        val existing = config(id)
        if (existing.isDefault) configurations.values.firstOrNull { it.id != id }?.let { ensureAvailable(it.id) }
        stopLocked(id)
        val root = File(existing.rootfsPath).canonicalFile
        // Registered developer/user-owned external trees are never recursively deleted.
        if (root.toPath().startsWith(rootfsDir.canonicalFile.toPath()) && root != rootfsDir.canonicalFile) {
            val ownership = RootfsOwnership.status(root)
            if (ownership?.physical == ContainerEngineType.CHROOT || ownership?.pending == true || (ownership == null && existing.engine == ContainerEngineType.CHROOT)) {
                RootfsOwnership.deleteRootfs(existing, engines::rootCommand)
            } else RootfsArchives.deleteTree(root)
        }
        configurations.remove(id)
        errors.remove(id)
        dnsErrors.remove(id)
        metadataErrors.remove(id)
        sizes.remove(id)
        sizeRequests.remove(id)
        if (id == "ubuntu-main") debugRootfsRemoved = true
        if (configurations.isNotEmpty() && configurations.values.none { it.isDefault }) {
            val first = configurations.values.first(); configurations[first.id] = first.copy(isDefault = true)
        }
        persist(); publish()
    } }

    override suspend fun clone(id: String, name: String): ContainerConfig = withContext(Dispatchers.IO) {
        ContainerMetadataStore.validate(ContainerMetadata(title = name))
        val owner = currentCoroutineContext()[Job]
        val newId = UUID.randomUUID().toString()
        val target = File(rootfsDir, newId)
        val archive = File(cacheDir, "$newId.clone.tar.zst")
        withStoppedRootfs(id, newId) { original -> trackOperation(id, original.displayName, "归档源容器") { report ->
            var registered = false
            var failure: Throwable? = null
            try {
                exportOwnedRootfs(original, archive, engines::rootCommand, startRootProcess = startRootProcess)
                currentCoroutineContext().ensureActive()
                report("安全解压克隆归档", null)
                val extractionContext = currentCoroutineContext()
                var lastUpdate = 0L
                RootfsArchives.extract(archive, target) { bytes ->
                    extractionContext.ensureActive()
                    val now = System.nanoTime()
                    if (now - lastUpdate > 100_000_000) { report("安全解压克隆归档 · ${bytes / 1024 / 1024} MB", null); lastUpdate = now }
                }
                currentCoroutineContext().ensureActive()
                val copiedConfig = original.copy(id = newId, name = name, rootfsPath = target.canonicalPath, isDefault = false)
                val stagingConfig = copiedConfig.copy(engine = ContainerEngineType.PROOT)
                val previous = metadataStore.read(stagingConfig)
                val metadata = metadataStore.save(stagingConfig, previous.value.copy(title = name), previous.revision)
                val copied = copiedConfig.copy(metadata = metadata.value)
                report("统计克隆容器体积", null)
                val size = sizeOf(copied, appOwnedRoot = true)
                currentCoroutineContext().ensureActive()
                if (copied.engine == ContainerEngineType.CHROOT && !copied.ownershipMigrationDeferred) {
                    val context = currentCoroutineContext()
                    RootfsOwnership.migrate(stagingConfig, copied, engines::rootCommand, report) { context.ensureActive() }
                }
                // Finish the commit/ownership handoff even if cancellation arrives after persist.
                withContext(NonCancellable) {
                    register(copied, initialTitle = null, measuredSize = size, reservation = id, owner = owner, onProgress = report).also { registered = true }
                }
            } catch (caught: Throwable) { failure = caught; throw caught }
            finally {
                var cleanupFailure = failure
                try { Files.deleteIfExists(archive.toPath()) }
                catch (cleanup: Throwable) { if (cleanupFailure == null) cleanupFailure = cleanup else cleanupFailure.addSuppressed(cleanup) }
                if (!registered) try { discardOwnedRootfs(target) }
                catch (cleanup: Throwable) { if (cleanupFailure == null) cleanupFailure = cleanup else cleanupFailure.addSuppressed(cleanup) }
                if (failure == null && cleanupFailure != null) throw cleanupFailure
            }
        }
        }
    }

    private data class RootfsOperation(
        var config: ContainerConfig,
        val runGeneration: Long?,
        val stopGeneration: Long,
        val reservedId: String?,
    )

    /** Busy containers reject new work, except explicit stop; the action never holds mutex. */
    internal suspend fun <T> withStoppedRootfs(id: String, reservedId: String? = null, restore: Boolean = true, action: suspend (ContainerConfig) -> T): T {
        val owner = currentCoroutineContext()[Job]
        val operation = mutex.withLock {
            currentCoroutineContext().ensureActive()
            ensureOpen(); ensureAvailable(id)
            val original = config(id)
            if (reservedId != null) {
                ensureAvailable(reservedId)
                require(reservedId !in configurations) { "容器 ID 已存在：$reservedId" }
            }
            val generation = runGenerations[id]?.takeIf { keepers[id]?.let { !it.stopping && it.isAlive } == true }
            stopLocked(id)
            RootfsOperation(original, generation, stopGenerations.getValue(id), reservedId).also {
                rootfsOperations[id] = it
                if (reservedId != null) rootfsOperations[reservedId] = it
            }
        }
        var failure: Throwable? = null
        try {
            currentCoroutineContext().ensureActive()
            return action(operation.config).also { currentCoroutineContext().ensureActive() }
        } catch (caught: Throwable) { failure = caught; throw caught }
        finally {
            try {
                withContext(NonCancellable) { mutex.withLock {
                    val current = rootfsOperations[id] === operation
                    if (current) rootfsOperations.remove(id)
                    operation.reservedId?.let { if (rootfsOperations[it] === operation) rootfsOperations.remove(it) }
                    val restore = restore && current && !closed && !closing && owner?.isActive != false && failure !is CancellationException &&
                        operation.runGeneration != null && configurations[id] === operation.config &&
                        stopGenerations[id] == operation.stopGeneration && runGenerations[id] == null
                    if (restore) {
                        try {
                            if (owner != null) withContext(owner) { startLocked(id) } else startLocked(id)
                        } catch (restoreFailure: Throwable) {
                            if (owner?.isActive == false) try { stopLocked(id, intentional = false) }
                            catch (cleanup: Throwable) { restoreFailure.addSuppressed(cleanup) }
                            throw restoreFailure
                        }
                    }
                } }
            } catch (cleanup: Throwable) { if (failure == null) throw cleanup else failure.addSuppressed(cleanup) }
        }
    }

    private fun progressReporter(id: String, name: String, onProgress: (DeploymentProgress) -> Unit = {}): (String, Float?) -> Unit = { phase, fraction ->
        val progress = DeploymentProgress(id, name, fraction, phase)
        deployment(progress)
        onProgress(progress)
    }

    private suspend fun <T> trackOperation(id: String, name: String, phase: String, block: suspend ((String, Float?) -> Unit) -> T): T {
        val report = progressReporter(id, name)
        report(phase, null)
        return try { block(report) } finally { finishDeployment(id) }
    }

    private fun deployment(progress: DeploymentProgress) { synchronized(_deployments) {
        val installer = distroDeployments[progress.id]
        val presented = if (installer == null) progress else if (installer.cancelling)
            progress.copy(progress = null, phase = "正在取消", error = null, cancellable = false, retryable = false)
        else progress.copy(cancellable = installer.owner != null && !installer.finalizing && !installer.failed,
            retryable = installer.failed)
        val current = _deployments.value
        val index = current.indexOfFirst { it.id == progress.id }
        if (index >= 0 && current[index] == presented) return@synchronized
        _deployments.value = current.toMutableList().apply {
            if (index >= 0) this[index] = presented else add(presented)
        }
    } }
    private fun finishDeployment(id: String) { synchronized(_deployments) { _deployments.value = _deployments.value.filterNot { it.id == id } } }

    override suspend fun installDistro(distro: DistroSpec, name: String, mirror: MirrorSource, engine: ContainerEngineType): ContainerConfig = withContext(Dispatchers.IO) {
        ensureOpen(); ContainerMetadataStore.validate(ContainerMetadata(title = name))
        val task = DistroDeployment(UUID.randomUUID().toString(), distro, name, mirror, engine)
        synchronized(_deployments) { ensureOpen(); distroDeployments[task.id] = task }
        runDistroDeployment(task, retry = false)
    }

    override suspend fun retryDistroDeployment(id: String): ContainerConfig = withContext(Dispatchers.IO) {
        ensureOpen()
        val task = synchronized(_deployments) { requireNotNull(distroDeployments[id]) { "下载任务已不存在" } }
        runDistroDeployment(task, retry = true)
    }

    override suspend fun cancelDeployment(id: String) = withContext(Dispatchers.IO) {
        val active = synchronized(_deployments) {
            val task = distroDeployments[id] ?: return@withContext
            if (task.owner == null || task.failed || task.finalizing || task.cancelling) return@withContext
            task.cancelling = true
            deployment(DeploymentProgress(id, task.name, null, "正在取消"))
            task.owner to task.transfer
        }
        active.first?.cancel(CancellationException("发行版部署已取消"))
        active.second?.cancel()
    }

    override suspend fun dismissDeployment(id: String) = withContext(Dispatchers.IO) {
        val owner = requireNotNull(currentCoroutineContext()[Job])
        val task = synchronized(_deployments) {
            val task = distroDeployments[id] ?: return@withContext
            check(task.failed && task.owner == null) { "请先等待下载任务结束" }
            task.owner = owner; task.failed = false; task.finalizing = true
            deployment(DeploymentProgress(id, task.name, null, "正在清理下载任务"))
            task
        }
        try {
            discardOwnedRootfs(File(rootfsDir, id))
            Files.deleteIfExists(File(cacheDir, "$id.part").toPath())
            synchronized(_deployments) { distroDeployments.remove(id); finishDeployment(id) }
        } catch (failure: Throwable) {
            synchronized(_deployments) {
                task.owner = null; task.failed = true; task.finalizing = false
                deployment(DeploymentProgress(id, task.name, null, "清理失败", failure.message ?: failure.toString()))
            }
            throw failure
        } finally {
            synchronized(_deployments) { if (task.owner === owner) task.owner = null }
        }
    }

    private suspend fun runDistroDeployment(task: DistroDeployment, retry: Boolean): ContainerConfig {
        val context = currentCoroutineContext()
        val owner = requireNotNull(context[Job])
        synchronized(_deployments) {
            ensureOpen()
            context.ensureActive()
            check(distroDeployments[task.id] === task && task.owner == null && (!retry || task.failed)) { "下载任务正在运行或已结束" }
            task.owner = owner; task.failed = false; task.cancelling = false; task.finalizing = false; task.committed = false
            task.transfer = DistroTransferControl { context.ensureActive() }
            deployment(DeploymentProgress(task.id, task.name, null, "读取发行者 SHA256 元数据"))
        }
        try {
            val source = resolveDistro(task.distro, task.mirror, requireNotNull(task.transfer))
            val installed = deploy(task, source)
            synchronized(_deployments) { distroDeployments.remove(task.id); finishDeployment(task.id) }
            return installed
        } catch (failure: Throwable) {
            val cancelled = failure is CancellationException || !context.isActive
            synchronized(_deployments) {
                task.owner = null; task.transfer = null
                if (task.committed || (cancelled && failure.suppressed.isEmpty())) {
                    distroDeployments.remove(task.id); finishDeployment(task.id)
                } else {
                    task.failed = true; task.cancelling = false; task.finalizing = false
                    val reason = if (cancelled) failure.suppressed.joinToString("\n") { it.message ?: it.toString() } else failure.message ?: failure.toString()
                    deployment(DeploymentProgress(task.id, task.name, null, if (cancelled) "取消清理失败" else "部署失败", "发行版部署失败：$reason"))
                }
            }
            if (cancelled) throw if (failure is CancellationException) failure
                else CancellationException("发行版部署已取消").apply { initCause(failure) }
            val message = "发行版部署失败：${failure.message}"
            throw IllegalStateException(message, failure)
        } finally {
            synchronized(_deployments) { if (task.owner === owner) { task.owner = null; task.transfer = null } }
        }
    }

    override suspend fun inspectRootfsArchive(source: String, name: String, engine: ContainerEngineType, expectedSha256: String?, onProgress: (DeploymentProgress) -> Unit): RootfsImportInspection {
        var preparedId: String? = null
        try {
            return withContext(Dispatchers.IO) { prepareRootfsImport(source, name, engine, expectedSha256, onProgress).also { preparedId = it.id } }
        } catch (failure: Throwable) {
            // withContext can discard an already prepared result when delivering cancellation.
            if (failure is CancellationException) preparedId?.let { discardRootfsImport(it) }
            throw failure
        }
    }

    private fun discardOwnedRootfs(root: File) {
        val manifest = RootfsOwnership.status(root)
        if (manifest?.physical == ContainerEngineType.CHROOT || manifest?.pending == true) {
            RootfsOwnership.deleteRootfs(ContainerConfig("discard", "discard", root.absolutePath, engine = ContainerEngineType.CHROOT), engines::rootCommand)
        } else RootfsArchives.deleteTree(root)
    }

    private fun discardImport(staged: StagedImport) {
        discardOwnedRootfs(staged.root)
        RootfsArchives.deleteTree(staged.directory)
    }

    private suspend fun prepareRootfsImport(source: String, name: String, engine: ContainerEngineType, expectedSha256: String?, onProgress: (DeploymentProgress) -> Unit): RootfsImportInspection {
        val requestedName = name.trim()
        ContainerMetadataStore.validate(ContainerMetadata(title = requestedName))
        val id = UUID.randomUUID().toString()
        val staged = StagedImport(id, requestedName, File(cacheDir, "import-$id"), currentCoroutineContext()[Job])
        mutex.withLock { ensureOpen(); rootfsImports[id] = staged }
        val report = progressReporter(id, requestedName, onProgress)
        var prepared = false
        return try {
            report("读取 rootfs 归档", null)
            staged.directory.mkdirs()
            var lastUpdate = 0L
            val context = currentCoroutineContext()
            DistroSources.transfer(source, staged.archive, expectedSha256) { bytes, total ->
                context.ensureActive()
                val now = System.nanoTime()
                if (now - lastUpdate > 100_000_000 || bytes == total) {
                    report("读取并校验归档 · ${bytes / 1024 / 1024} MB${if (total > 0) " / ${total / 1024 / 1024} MB" else ""}", if (total > 0) bytes.toFloat() / total else null)
                    lastUpdate = now
                }
            }
            report("安全解压 rootfs", null)
            RootfsArchives.extract(staged.archive, staged.root) { bytes ->
                context.ensureActive()
                val now = System.nanoTime()
                if (now - lastUpdate > 100_000_000) { report("安全解压 · ${bytes / 1024 / 1024} MB", null); lastUpdate = now }
            }
            report("检查发行版、ELF 与容器信息", null)
            val os = readOsRelease(staged.root)
            val distro = when (os["ID"]) { "archarm", "arch" -> "arch"; else -> os["ID"] ?: "custom" }
            val candidate = ContainerConfig(id, requestedName, staged.root.canonicalPath, distro, os["VERSION_ID"] ?: "custom", detectArch(staged.root), engine,
                link2symlink = paths.isAndroid, shell = if (File(staged.root, "bin/bash").let { it.isFile && it.canExecute() }) "/bin/bash" else "/bin/sh")
            engines.validateRoot(candidate)
            // Invalid portable metadata remains repairable, as for ordinary offline registration.
            val metadata = try { metadataStore.read(candidate.copy(engine = ContainerEngineType.PROOT)).value } catch (_: Exception) { ContainerMetadata() }
            val inspection = RootfsImportInspection(id, requestedName, metadata)
            currentCoroutineContext().ensureActive()
            Files.deleteIfExists(staged.archive.toPath())
            mutex.withLock {
                ensureOpen(); check(!staged.discarded) { "导入已取消" }
                staged.config = candidate
                staged.inspection = inspection
                staged.owner = null
                prepared = true
            }
            finishDeployment(id)
            inspection
        } catch (failure: Throwable) {
            if (failure !is CancellationException) deployment(DeploymentProgress(id, requestedName, 0f, "导入检查失败", failure.message))
            throw failure
        } finally {
            if (!prepared) withContext(NonCancellable) {
                mutex.withLock { rootfsImports.remove(id) }
                discardImport(staged)
                if (staged.discarded || staged.owner?.isActive == false) finishDeployment(id)
            }
        }
    }

    override suspend fun discardRootfsImport(inspectionId: String) = withContext(Dispatchers.IO + NonCancellable) {
        val staged = mutex.withLock {
            rootfsImports.remove(inspectionId)?.also { it.discarded = true; it.owner?.cancel(CancellationException("导入已取消")) }
        }
        if (staged != null && staged.owner == null) discardImport(staged)
        finishDeployment(inspectionId)
    }

    override suspend fun importRootfs(inspectionId: String, name: String, mode: RootfsImportMode, targetId: String?, onProgress: (DeploymentProgress) -> Unit): ContainerConfig = withContext(Dispatchers.IO) {
        val requestedName = name.trim()
        ContainerMetadataStore.validate(ContainerMetadata(title = requestedName))
        val owner = currentCoroutineContext()[Job]
        val staged = mutex.withLock {
            ensureOpen()
            val staged = rootfsImports[inspectionId] ?: error("导入检查已失效，请重新选择归档")
            check(staged.owner == null && staged.inspection != null) { "归档正在处理中" }
            val conflicts = rootfsImportConflicts(requestedName, if (mode == RootfsImportMode.RENAME) "" else staged.inspection!!.metadata.title, configurations.values.toList())
            if (mode == RootfsImportMode.UPDATE) require(targetId != null && conflicts.any { it.id == targetId }) { "必须选择实际冲突的容器进行更新" }
            else { require(targetId == null) { "新容器不能指定覆盖目标" }; require(conflicts.isEmpty()) { "容器名称已存在，请选择覆盖更新或重命名" } }
            staged.owner = owner
            staged
        }
        var committed = false
        val report = progressReporter(inspectionId, requestedName, onProgress)
        try {
            report(if (mode == RootfsImportMode.UPDATE) "校验覆盖计划（保留工作区和凭据）" else "保存容器配置", null)
            val candidate = requireNotNull(staged.config)
            val result = if (mode == RootfsImportMode.UPDATE) {
                withStoppedRootfs(requireNotNull(targetId)) { original ->
                    mutex.withLock {
                        ensureOpen(); check(!staged.discarded)
                        require(rootfsImportConflicts(requestedName, staged.inspection!!.metadata.title, configurations.values.toList()).any { it.id == original.id }) { "所选容器不再与导入名称冲突" }
                    }
                    RootfsOwnership.requireStable(original)
                    engines.validateRoot(original)
                    engines.validateRoot(candidate.copy(engine = original.engine))
                    require(candidate.arch == engines.guestArch(original.arch)) { "更新归档的 ELF 架构与原容器配置不兼容" }
                    val physicalConfig = original.copy(engine = RootfsOwnership.physical(original))
                    val privileged = physicalConfig.engine == ContainerEngineType.CHROOT
                    if (privileged) report("扫描原容器文件属主", null)
                    val snapshot = if (privileged) RootfsOwnership.snapshot(original, engines::rootCommand) else null
                    val homes = snapshot?.let { RootfsOwnership.exportHomes(original, it, engines::rootCommand) }
                    val home = if (privileged) RootfsOwnership.managedHome(original, engines::rootCommand) else containerHome(original)
                    val overlay = RootfsOverlay(candidate, original, snapshot, homes, home)
                    currentCoroutineContext().ensureActive()
                    val mapped = original.copy(engine = ContainerEngineType.PROOT)
                    if (privileged) RootfsOwnership.migrate(original, mapped, engines::rootCommand, report)
                    var overlayFailure: Throwable? = null
                    try { report("安全合并容器文件", null); overlay.apply() }
                    catch (failure: Throwable) { overlayFailure = failure; throw failure }
                    finally {
                        if (privileged) try {
                            // Restore only ownership, never start a guest after cancellation or manual stop.
                            withContext(NonCancellable) { RootfsOwnership.migrate(mapped, physicalConfig, engines::rootCommand, report) }
                        } catch (failure: Throwable) { if (overlayFailure != null) overlayFailure.addSuppressed(failure) else throw failure }
                    }
                    report("统计容器体积", null)
                    val measured = sizeOf(original)
                    currentCoroutineContext().ensureActive()
                    mutex.withLock {
                        ensureOpen(); check(!staged.discarded)
                        check(configurations[original.id] === original)
                        refreshMetadata(original)
                        val updated = configurations.getValue(original.id)
                        // The lease restores the refreshed identity, not the stale pre-update snapshot.
                        rootfsOperations.getValue(original.id).config = updated
                        sizes[original.id] = measured
                        sizeRequests.remove(original.id)
                        persist(); publish()
                        updated
                    }
                }
            } else {
                val stored = RootfsOwnership.status(staged.root)
                if (stored?.pending == true) {
                    val context = currentCoroutineContext()
                    RootfsOwnership.migrate(candidate, candidate, engines::rootCommand, report) { context.ensureActive() }
                }
                val physical = RootfsOwnership.status(staged.root)?.physical
                val stagingConfig = candidate.copy(engine = physical ?: ContainerEngineType.PROOT)
                val snapshot = try { metadataStore.read(stagingConfig) } catch (failure: Exception) {
                    if (mode == RootfsImportMode.RENAME) throw failure
                    null
                }
                if (snapshot != null) {
                    val title = if (mode == RootfsImportMode.RENAME) requestedName else staged.inspection!!.metadata.title.ifBlank { requestedName }
                    if (snapshot.value.title != title || snapshot.revision == null) metadataStore.save(stagingConfig, snapshot.value.copy(title = title), snapshot.revision)
                }
                report("统计容器体积", null)
                val measured = sizeOf(candidate, appOwnedRoot = physical != ContainerEngineType.CHROOT)
                currentCoroutineContext().ensureActive()
                val destination = File(rootfsDir, inspectionId)
                var registered = false
                Files.move(staged.root.toPath(), destination.toPath(), StandardCopyOption.ATOMIC_MOVE)
                RootfsOwnership.moveManifest(staged.root, destination)
                try {
                    withContext(NonCancellable) {
                        register(candidate.copy(name = requestedName, rootfsPath = destination.canonicalPath), initialTitle = null, measuredSize = measured, owner = owner,
                            importNames = requestedName to if (mode == RootfsImportMode.RENAME) "" else staged.inspection!!.metadata.title, onProgress = report).also { registered = true }
                    }
                } finally {
                    if (!registered) {
                        Files.move(destination.toPath(), staged.root.toPath(), StandardCopyOption.ATOMIC_MOVE)
                        RootfsOwnership.moveManifest(destination, staged.root)
                    }
                }
            }
            committed = true
            finishDeployment(inspectionId)
            result
        } catch (failure: Throwable) {
            if (failure !is CancellationException) deployment(DeploymentProgress(inspectionId, requestedName, .85f, "导入失败，可重试", failure.message))
            throw failure
        } finally {
            withContext(NonCancellable) {
                val cleanup = mutex.withLock {
                    staged.owner = null
                    if (committed || staged.discarded || closing || closed || owner?.isActive == false) {
                        rootfsImports.remove(inspectionId)
                        true
                    } else false
                }
                if (cleanup) { discardImport(staged); finishDeployment(inspectionId) }
            }
        }
    }

    private suspend fun deploy(task: DistroDeployment, download: DistroDownload): ContainerConfig {
        val id = task.id; val name = task.name; val distro = task.distro; val mirror = task.mirror
        val control = requireNotNull(task.transfer)
        val archive = File(cacheDir, "$id.part")
        val target = File(rootfsDir, id)
        try {
            control.checkActive()
            // A failed attempt may have left an owned partial tree after a cleanup error.
            discardOwnedRootfs(target)
            var lastUpdate = 0L
            DistroSources.transfer(download.url, archive, download.sha256, control) { bytes, total ->
                control.checkActive()
                val now = System.nanoTime()
                if (now - lastUpdate > 100_000_000 || bytes == total) {
                    val totalLabel = if (total > 0) " / ${total / 1024 / 1024} MB" else ""
                    deployment(DeploymentProgress(id, name, if (total > 0) (bytes.toFloat() / total).coerceIn(0f, 1f) else null, "${download.phase} · ${bytes / 1024 / 1024} MB$totalLabel"))
                    lastUpdate = now
                }
            }
            deployment(DeploymentProgress(id, name, null, "安全解压 rootfs"))
            RootfsArchives.extract(archive, target, checkActive = control::checkActive) { bytes ->
                control.checkActive()
                val now = System.nanoTime()
                if (now - lastUpdate > 100_000_000) { deployment(DeploymentProgress(id, name, null, "安全解压中 · ${bytes / 1024 / 1024} MB")); lastUpdate = now }
            }
            readOsRelease(target)
            val distroId = distro.id
            DistroSources.configure(target, distroId, mirror)
            val config = ContainerConfig(id, name, target.canonicalPath, distroId, distro.version, distro.arch, task.engine, link2symlink = paths.isAndroid, shell = if (File(target, "bin/bash").exists()) "/bin/bash" else "/bin/sh")
            engines.validateRoot(config)
            synchronized(_deployments) {
                control.checkActive()
                task.finalizing = true
                deployment(DeploymentProgress(id, name, null, "保存容器配置"))
            }
            // From this point the container can become durable. A late cancellation must not
            // delete a registered rootfs or report a completed installation as retryable.
            return withContext(NonCancellable) {
                register(config, initialTitle = name, onProgress = progressReporter(id, name)).also { task.committed = true }
            }
        } catch (failure: Throwable) {
            if (!task.committed) try { discardOwnedRootfs(target) } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            throw failure
        }
        finally { archive.delete() }
    }

    private fun readOsRelease(root: File): Map<String, String> {
        val file = File(root, "etc/os-release")
        require(file.canonicalFile.toPath().startsWith(root.canonicalFile.toPath())) { "os-release 路径越界" }
        return file.takeIf { it.isFile }?.readLines()?.filter { it.contains('=') && !it.startsWith('#') }?.associate { it.substringBefore('=') to it.substringAfter('=').trim().trim('"', '\'') }.orEmpty()
    }
    private fun detectArch(root: File): String {
        val shell = File(root, "bin/sh").canonicalFile
        require(shell.toPath().startsWith(root.canonicalFile.toPath())) { "Shell 链接逃逸 rootfs" }
        require(shell.isFile && shell.canExecute()) { "导入的 /bin/sh 必须是可执行普通文件" }
        val header = shell.inputStream().use { stream -> ByteArray(20).also { check(stream.read(it) == 20) { "无法读取 Shell ELF 架构" } } }
        require(header.take(4) == listOf(0x7f.toByte(), 'E'.code.toByte(), 'L'.code.toByte(), 'F'.code.toByte())) { "导入的 /bin/sh 不是 ELF 程序" }
        val machine = if (header[5].toInt() == 1) (header[18].toInt() and 255) or ((header[19].toInt() and 255) shl 8) else ((header[18].toInt() and 255) shl 8) or (header[19].toInt() and 255)
        return when (machine) { 183 -> "arm64"; 62 -> "amd64"; 40 -> "arm"; else -> error("不支持的 ELF 架构：$machine") }
    }

    override suspend fun exportRootfs(id: String, destination: String, sanitization: RootfsExportSanitization) = withContext(Dispatchers.IO) {
        val target = if (destination.startsWith("file:")) File(java.net.URI(destination)) else File(destination)
        require(target.name.endsWith(".tar.zst")) { "导出文件名必须以 .tar.zst 结尾" }
        require(!target.exists()) { "导出文件已存在，请选择新文件名" }
        val temporary = File(target.absoluteFile.parentFile, ".${target.name}.${UUID.randomUUID()}.part")
        withStoppedRootfs(id) { config -> trackOperation(id, config.displayName, "导出 Rootfs 归档") { report ->
            var failure: Throwable? = null
            try {
                exportOwnedRootfs(config, temporary, engines::rootCommand, sanitization = sanitization, startRootProcess = startRootProcess)
                currentCoroutineContext().ensureActive()
                report("提交 Rootfs 归档", null)
                Files.move(temporary.toPath(), target.toPath())
                Unit
            } catch (caught: Throwable) {
                val reported = if (caught is CancellationException) caught else IllegalStateException("rootfs 导出失败：${caught.message}", caught)
                failure = reported
                throw reported
            } finally {
                try { Files.deleteIfExists(temporary.toPath()) }
                catch (cleanup: Throwable) { if (failure == null) throw cleanup else failure.addSuppressed(cleanup) }
            }
        }
        }
    }

    override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
        val generation = containers.value.firstOrNull { it.config.id == id }?.stopGeneration
        val operation = currentCoroutineContext()[ContainerFileOperation]?.takeIf { it.containerId == id }
        return withContext(Dispatchers.IO) {
            mutex.withLock {
                currentCoroutineContext().ensureActive()
                if (generation != null && wasInterrupted(id, generation)) throw CancellationException("容器已停止或重新启动")
                if (operation != null && !operation.isCurrent(this@LocalContainerRuntime)) throw CancellationException("容器已停止或重新启动")
                ensureOpen()
                ensureAvailable(id)
                if (operation?.runGeneration == null) startLocked(id)
                else if (keepers[id]?.let { !it.stopping && it.isAlive } != true) throw CancellationException("容器已停止")
                if (operation != null) operation.runGeneration = runGenerations[id]
                idleJobs.remove(id)?.cancel()
                launchLocked(config(id), arguments, cwd, environment, false, false).process
            }
        }
    }

    override suspend fun openRunningProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
        val generation = containers.value.firstOrNull { it.config.id == id }?.stopGeneration
        val operation = currentCoroutineContext()[ContainerFileOperation]?.takeIf { it.containerId == id }
        return withContext(Dispatchers.IO) {
            mutex.withLock {
                currentCoroutineContext().ensureActive()
                if (generation != null && wasInterrupted(id, generation)) throw CancellationException("容器已停止或重新启动")
                if (operation != null && !operation.isCurrent(this@LocalContainerRuntime)) throw CancellationException("容器已停止或重新启动")
                ensureOpen()
                ensureAvailable(id)
                check(keepers[id]?.let { !it.stopping && it.isAlive } == true) { "容器已停止" }
                if (operation != null) operation.runGeneration = runGenerations[id]
                idleJobs.remove(id)?.cancel()
                launchLocked(config(id), arguments, cwd, environment, false, false).process
            }
        }
    }

    override suspend fun closeProcess(id: String, process: Process): Unit = withContext(NonCancellable + Dispatchers.IO) {
        val target = mutex.withLock {
            val tracked = processes[id]?.firstOrNull { it.process === process }
            if (tracked == null) {
                check(!process.isAlive) { "容器命令不属于指定容器，拒绝清理未知进程" }
                return@withLock null
            }
            check(!tracked.keeper) { "不能通过命令清理接口关闭容器服务" }
            tracked.stopping = true
            tracked to (configurations[id]?.engine == ContainerEngineType.CHROOT)
        } ?: return@withContext
        val (tracked, root) = target
        // Serialize repeated closes without holding the runtime mutex during Root I/O or waits.
        // A concurrent container stop may also signal these same pinned identities safely.
        tracked.cleanupMutex.withLock {
            tracked.outputDrainer?.cancel()
            tracked.refreshDescendants()
            terminate(listOf(tracked), root, false)
            if (process.isAlive) process.destroy()
            waitForTrackedExit(listOf(tracked), 2)
            tracked.refreshDescendants()
            terminate(listOf(tracked), root, true)
            if (process.isAlive) process.destroyForcibly()
            waitForTrackedExit(listOf(tracked), 2)
            tracked.terminal?.close()
            check(!tracked.hasLiveProcesses()) { "容器命令仍有已跟踪子进程未退出，已保留进程身份供重试" }
            mutex.withLock {
                // Stop/delete may already have removed this record. Never touch a newer run.
                processes[id]?.let { records ->
                    records.remove(tracked)
                    if (records.isEmpty()) processes.remove(id)
                }
            }
        }
    }

    private fun launchLocked(config: ContainerConfig, arguments: List<String>, cwd: String, environment: Map<String, String>, merge: Boolean, keeper: Boolean, pty: Boolean = false): TrackedProcess {
        val root = config.engine == ContainerEngineType.CHROOT
        val pidFile = if (root) File(paths.dataDir, "tmp/root-${UUID.randomUUID()}.pid").apply { parentFile.mkdirs(); check(createNewFile()) { "无法创建 Root PID 握手文件" } } else null
        var terminal: PtyTransport? = null
        val process = try {
            val builder = engines.builder(config, arguments, cwd, environment, merge, pidFile)
            if (pty) createPtyTransport(builder).also { terminal = it }.process else builder.start()
        }
        catch (failure: Throwable) { pidFile?.delete(); throw IllegalStateException("无法启动容器引擎：${failure.message}", failure) }
        val identity = if (pidFile != null) {
            var result: Pair<Long, String>? = null
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
            while (System.nanoTime() < deadline && result == null && process.isAlive) {
                result = TrackedProcess.handshake(pidFile)
                if (result == null) Thread.sleep(20)
            }
            result ?: TrackedProcess.handshake(pidFile)
        } else null
        if (root && identity == null) {
            process.destroyForcibly(); terminal?.close(); pidFile?.delete()
            error("无法安全识别 su 启动的 chroot PID；执行确认未发送，Root Shell 将超时退出")
        }
        val tracked = try {
            TrackedProcess(process, keeper, scope, identity, if (root) engines::rootSnapshot else null, terminal,
                ownsProcessGroup = !root && pty && paths.isAndroid)
        }
        catch (failure: Throwable) { process.destroyForcibly(); terminal?.close(); pidFile?.delete(); throw failure }
        if (tracked.pid == null) { process.destroyForcibly(); terminal?.close(); pidFile?.delete(); error("无法识别容器进程 PID，拒绝启动以避免遗留进程") }
        processes.getOrPut(config.id) { mutableListOf() } += tracked
        if (keeper) tracked.outputDrainer = scope.launch {
            try {
                process.inputStream.use { input ->
                    val buffer = ByteArray(4096)
                    while (input.read(buffer) >= 0) { /* Drain only; keeper has no terminal consumer. */ }
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Throwable) {
                // A SupervisorJob does not consume launch failures: never rethrow pipe closure.
                mutex.withLock {
                    if (!closed && !tracked.stopping && tracked.isAlive && keepers[config.id] === tracked) {
                        val reason = "容器引擎输出读取失败：${failure.message ?: failure.javaClass.simpleName}"
                        try { stopLocked(config.id, intentional = false) }
                        catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
                        errors[config.id] = reason
                        publish()
                    }
                }
            }
        }
        scope.launch {
            try {
                while (!tracked.stopping) {
                    tracked.refreshDescendants()
                    if (if (keeper) !tracked.isAlive else !tracked.hasLiveProcesses()) break
                    delay(300)
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                mutex.withLock {
                    if (!tracked.stopping && processes[config.id]?.contains(tracked) == true) {
                        errors[config.id] = "无法安全跟踪容器进程：${failure.message}"; publish()
                    }
                }
                return@launch
            }
            mutex.withLock {
                // Failed stops retain identity records for retry; never discard them on EOF.
                if (tracked.stopping || closed) return@withLock
                if (keeper && keepers[config.id] === tracked) {
                    val reason = "容器引擎进程已退出（${process.exitValue()}）"
                    try { stopLocked(config.id, intentional = false); errors[config.id] = reason }
                    catch (failure: Throwable) { errors[config.id] = "$reason；${failure.message}" }
                    publish()
                } else {
                    processes[config.id]?.remove(tracked)
                    if (!keeper && configurations[config.id]?.autoSleep == true && processes[config.id].orEmpty().none { !it.keeper }) {
                        val stopGeneration = stopGenerations[config.id]
                        val keeperAtSchedule = keepers[config.id]
                        idleJobs[config.id] = scope.launch {
                            delay(30_000)
                            mutex.withLock { stopIdleLocked(config.id, stopGeneration, keeperAtSchedule) }
                        }
                    }
                }
            }
        }
        if (pidFile != null) {
            try {
                check(File(pidFile.path + ".ack").createNewFile()) { "无法确认 Root 进程执行" }
            } catch (failure: Throwable) {
                // Tracking is installed before acknowledgement, including when filesystem I/O fails.
                try { terminate(listOf(tracked), root, true) } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
                process.destroyForcibly()
                try { terminal?.close() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
                throw failure
            } finally { pidFile.delete() }
        }
        return tracked
    }

    override suspend fun execute(id: String, command: String, cwd: String): String {
        val generation = containers.value.firstOrNull { it.config.id == id }?.stopGeneration
        try {
            val shell = mutex.withLock { config(id).shell }
            if (generation != null && wasInterrupted(id, generation)) throw CancellationException("容器已停止或重新启动")
            val process = openProcess(id, listOf(shell, "-lc", command), cwd)
            return collect(process)
        } catch (failure: Exception) {
            currentCoroutineContext().ensureActive()
            if (generation != null && wasInterrupted(id, generation)) throw CancellationException("容器已停止或重新启动", failure)
            throw failure
        }
    }
    private suspend fun collect(process: Process): String = coroutineScope {
        val stdout = async(Dispatchers.IO) { process.inputStream.bufferedReader().use { it.readText() } }
        val stderr = async(Dispatchers.IO) { process.errorStream.bufferedReader().use { it.readText() } }
        try {
            val code = withContext(Dispatchers.IO) { process.waitFor() }
            val output = stdout.await() + stderr.await()
            check(code == 0) { "容器命令失败（$code）：${output.trim()}" }
            output
        } finally { if (process.isAlive) process.destroy() }
    }

    override suspend fun openTerminal(id: String, initialCommand: String?): TerminalSession {
        val generation = containers.value.firstOrNull { it.config.id == id }?.stopGeneration
        return withContext(Dispatchers.IO) { mutex.withLock {
            currentCoroutineContext().ensureActive()
            if (generation != null && wasInterrupted(id, generation)) throw CancellationException("容器已停止或重新启动")
            ensureOpen(); startLocked(id); idleJobs.remove(id)?.cancel()
            val config = config(id)
            val tracked = launchLocked(config, terminalArguments(config.shell, initialCommand), "/workspace", emptyMap(), true, false, pty = true)
            object : TerminalSession {
                override val process = tracked.process
                override val processId = requireNotNull(tracked.pid)
                private val alive = MutableStateFlow(process.isAlive)
                override val isAlive = alive.asStateFlow()
                private val closing = java.util.concurrent.atomic.AtomicBoolean()
                init { scope.launch { process.waitFor(); alive.value = false } }
                override fun resize(columns: Int, rows: Int, cellWidthPixels: Int, cellHeightPixels: Int) {
                    require(columns > 0 && rows > 0)
                    if (!closing.get() && !tracked.stopping && process.isAlive) {
                        try { requireNotNull(tracked.terminal).resize(columns, rows, cellWidthPixels, cellHeightPixels) }
                        catch (failure: Exception) { if (!closing.get() && !tracked.stopping) throw failure }
                    }
                }
                override fun close() {
                    if (!closing.compareAndSet(false, true)) return
                    scope.launch {
                        mutex.withLock {
                            val root = config.engine == ContainerEngineType.CHROOT
                            try {
                                tracked.refreshDescendants()
                                terminate(listOf(tracked), root, false)
                                if (process.isAlive) process.destroy()
                                process.waitFor(2, TimeUnit.SECONDS)
                                terminate(listOf(tracked), root, true)
                                if (process.isAlive) process.destroyForcibly()
                                waitForTrackedExit(listOf(tracked), 2)
                                check(!tracked.hasLiveProcesses()) { "终端仍有已跟踪子进程未退出" }
                            } catch (failure: Exception) {
                                errors[id] = "终端关闭失败：${failure.message}"
                                publish()
                            } finally {
                                try { tracked.terminal?.close() }
                                catch (failure: Exception) { errors[id] = "终端 PTY 关闭失败：${failure.message}"; publish() }
                                alive.value = false
                            }
                        }
                    }
                }
            }
        } }
    }

    override suspend fun probeToolchains(id: String): List<ToolchainInfo> = tools.map { tool ->
        val output = execute(id, "if command -v ${tool.probe} >/dev/null 2>&1; then ${tool.version} 2>&1 | head -n 1; else printf '__WAWLINK_MISSING__'; fi", "/")
        val missing = output.contains("__WAWLINK_MISSING__")
        val version = if (missing) "" else output.trim().let { if (tool.id == "git") it.removePrefix("git version ") else it }
        ToolchainInfo(tool.id, tool.title, tool.icon, !missing, version)
    }

    override suspend fun changeToolchain(id: String, toolId: String, install: Boolean): String = packageLocks.getOrPut(id) { Mutex() }.withLock {
        val tool = tools.firstOrNull { it.id == toolId } ?: error("未知工具链：$toolId")
        val distro = mutex.withLock { config(id).distro }
        val manager = manager(distro)
        val packages = tool.packages.getValue(manager)
        val command = PackageManagerCommands.toolchain(manager, packages, install)
        val result = StringBuilder(execute(id, command, "/"))
        if (toolId == "node" && !install) {
            result.append(execute(id, """
                marker=/usr/local/share/wawlink/preinstalled-node
                if [ -f "${'$'}marker" ] && [ -x /usr/local/bin/node ] && [ "${'$'}(cat "${'$'}marker")" = "${'$'}(/usr/local/bin/node --version)" ]; then
                    rm -f /usr/local/bin/node /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack
                    rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack /usr/local/include/node /usr/local/share/doc/node
                    rm -f /usr/local/share/man/man1/node.1 "${'$'}marker"
                fi
            """.trimIndent(), "/"))
        }
        val installed = execute(id, "if command -v ${tool.probe} >/dev/null 2>&1; then printf installed; fi", "/").trim() == "installed"
        check(installed == install) {
            if (install) "软件包命令已完成，但 ${tool.title} 仍不可执行，请查看包管理器输出：$result"
            else "包管理器已卸载其管理的文件，但 ${tool.title} 仍在其他路径中；无法安全删除未知的手动安装，请在终端检查 command -v ${tool.probe}"
        }
        mutex.withLock { ensureOpen(); scheduleSize(config(id)) }
        result.toString()
    }

    override suspend fun cleanPackageCaches(): Long {
        val ids = mutex.withLock { ensureOpen(); configurations.keys.toList() }
        var total = 0L
        ids.forEach { id -> packageLocks.getOrPut(id) { Mutex() }.withLock {
            val manager = manager(mutex.withLock { config(id).distro })
            val cache = when (manager) { "apt" -> "/var/cache/apt/archives"; "apk" -> "/var/cache/apk"; "pacman" -> "/var/cache/pacman/pkg"; else -> "/var/cache/dnf" }
            val clean = PackageManagerCommands.clean(manager)
            val result = execute(id, "before=\$(du -sk $cache 2>/dev/null | cut -f1); $clean && after=\$(du -sk $cache 2>/dev/null | cut -f1) && printf '\\n__WAWLINK_FREED__%s\\n' \"\$(( (\${before:-0} - \${after:-0}) * 1024 ))\"", "/")
            val freed = Regex("__WAWLINK_FREED__(-?[0-9]+)").find(result)?.groupValues?.get(1)?.toLongOrNull() ?: error("无法读取实际缓存清理结果")
            total += freed.coerceAtLeast(0)
            mutex.withLock { ensureOpen(); scheduleSize(config(id)) }
        } }
        return total
    }
    private fun manager(distro: String): String = when (distro) {
        "ubuntu", "debian", "kali" -> "apt"; "alpine" -> "apk"; "arch", "archlinux", "archarm" -> "pacman"; "fedora" -> "dnf"; else -> error("发行版 $distro 未配置受支持的包管理器")
    }

    override fun close() {
        runBlocking(Dispatchers.IO) { mutex.withLock {
            if (closed) return@withLock
            closing = true
            scope.cancel()
            synchronized(_deployments) { distroDeployments.values.toList() }.forEach { task ->
                task.owner?.cancel(CancellationException("容器运行时已关闭"))
                task.transfer?.cancel()
            }
            val failures = mutableListOf<Throwable>()
            rootfsImports.values.toList().forEach { staged ->
                staged.discarded = true
                staged.owner?.cancel(CancellationException("容器运行时已关闭"))
                if (staged.owner == null) try { discardImport(staged) } catch (failure: Throwable) { failures += failure }
                finishDeployment(staged.id)
            }
            rootfsImports.clear()
            configurations.keys.toList().forEach { id ->
                try { stopLocked(id) }
                catch (failure: Throwable) { failures += failure; errors[id] = failure.message ?: "容器关闭失败"; publish() }
            }
            if (failures.isNotEmpty()) throw IllegalStateException("部分容器未能安全关闭；请恢复 Root 权限后重试，进程与挂载记录已保留", failures.first()).also { error -> failures.drop(1).forEach(error::addSuppressed) }
            closed = true
        } }
    }

    private data class Tool(val id: String, val title: String, val icon: String, val probe: String, val version: String, val packages: Map<String, String>)
    private companion object {
        fun packages(apt: String, apk: String = apt, pacman: String = apt, dnf: String = apt) = mapOf("apt" to apt, "apk" to apk, "pacman" to pacman, "dnf" to dnf)
        val tools = listOf(
            Tool("git", "Git", "account_tree", "git", "git --version", packages("git")),
            Tool("node", "Node.js", "javascript", "node", "node --version", packages("nodejs npm")),
            Tool("python", "Python", "code", "python3", "python3 --version", packages("python3 python3-pip", "python3 py3-pip", "python python-pip", "python3 python3-pip")),
            Tool("rust", "Rust", "settings", "rustc", "rustc --version", packages("rustc cargo", "rust cargo", "rust", "rust cargo")),
            Tool("go", "Go", "code_blocks", "go", "go version", packages("golang", "go", "go", "golang")),
            Tool("jdk", "JDK", "coffee", "java", "java -version", packages("default-jdk", "openjdk21", "jdk-openjdk", "java-21-openjdk-devel")),
            Tool("cpp", "C++", "terminal", "g++", "g++ --version", packages("build-essential", "build-base", "base-devel", "gcc-c++ make")),
            Tool("neovim", "Neovim", "edit_note", "nvim", "nvim --version", packages("neovim")),
        )
    }
}


/** Starttime pins ownership even when a PID is recycled. Root commands always require
 * privileged identity snapshots. An ordinary child may be opaque on Android: only its
 * held, still-live Process authorizes the unverified root, never a later PID occupant. */
internal class TrackedProcess(
    val process: Process,
    val keeper: Boolean,
    ownerScope: CoroutineScope,
    rootIdentity: Pair<Long, String>? = null,
    private val privilegedSnapshot: ((List<Long>) -> List<String>)? = null,
    val terminal: PtyTransport? = null,
    // Only the Android Termux fork/setsid transport supplies this ownership guarantee.
    private val ownsProcessGroup: Boolean = false,
    private val statReader: (Long) -> String? = { File("/proc/$it/stat").readText() },
    private val groupSignal: (Long, Boolean) -> Unit = { pid, force ->
        // Signal directly: spawning a shell would widen the exit/PID-reuse race.
        try {
            Class.forName("android.system.Os").getMethod("kill", Int::class.javaPrimitiveType, Int::class.javaPrimitiveType)
                .invoke(null, -pid.toInt(), if (force) 9 else 15)
        } catch (failure: java.lang.reflect.InvocationTargetException) {
            val cause = failure.targetException
            val errno = runCatching { cause.javaClass.getField("errno").getInt(cause) }.getOrNull()
            if (errno != 3) throw IllegalStateException("无法终止终端进程组", cause) // ESRCH: already exited.
        }
        Unit
    },
) {
    @Volatile var stopping = false
    val cleanupMutex = Mutex()
    var outputDrainer: Job? = null
    val pid: Long? = rootIdentity?.first ?: findPid(process)
    private val identities = java.util.concurrent.ConcurrentHashMap<Long, String>()
    private var unverifiedRoot: Long? = if (rootIdentity == null) pid else null
    @Volatile private var processExited = !process.isAlive
    @Volatile private var exitFailure: Exception? = null
    private val exitWatcher: Job
    private var rootLive = emptyList<Long>()
    private var snapshotTime = 0L
    init {
        require(privilegedSnapshot == null || rootIdentity != null) { "Root 进程必须提供已验证身份" }
        if (rootIdentity != null) identities[rootIdentity.first] = rootIdentity.second
        exitWatcher = ownerScope.launch(exitDispatcher) {
            try {
                runInterruptible { process.waitFor() }
                processExited = true
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (failure: Exception) {
                exitFailure = failure
            }
        }
        try { refreshDescendants() } catch (failure: Throwable) { exitWatcher.cancel(); throw failure }
    }

    val isAlive: Boolean get() = originalIsAlive()

    private fun originalIsAlive(): Boolean {
        if (processExited) return false
        // Runtime close cancels waiters before reclaiming processes; cleanup still needs fresh truth.
        if (exitWatcher.isCancelled || exitFailure != null) return confirmOriginalIsAlive()
        return !processExited
    }

    /** Cached liveness never authorizes signaling an opaque/recycled PID or adopting its stat. */
    private fun confirmOriginalIsAlive(): Boolean {
        if (!processExited && !process.isAlive) processExited = true
        return !processExited
    }

    private fun readStat(pid: Long): ProcStat? = try {
        statReader(pid)?.let(::parseStat)?.takeIf { it.pid == pid }
    } catch (_: Exception) { null }

    private fun ownsPid(candidate: Long): Boolean {
        if (candidate == pid && !originalIsAlive()) return false
        if (candidate == unverifiedRoot) return confirmOriginalIsAlive()
        val start = identities[candidate] ?: return false
        return readStat(candidate)?.let { it.start == start && it.state != "Z" } == true
    }

    /** No PID-only fallback for descendants or privileged chroot identities. */
    @Synchronized fun terminateFallback(force: Boolean) {
        if (privilegedSnapshot != null || pid == null || !confirmOriginalIsAlive()) return
        val stat = readStat(pid)
        val start = identities[pid]
        if (stat != null && start != null && stat.start != start) return
        if (ownsProcessGroup && (stat == null || stat.group == pid && stat.session == pid) && confirmOriginalIsAlive()) {
            groupSignal(pid, force)
        }
        if ((stat == null || start == null) && confirmOriginalIsAlive()) {
            if (force) process.destroyForcibly() else process.destroy()
        }
    }

    @Synchronized fun refreshDescendants() {
        exitFailure?.let { throw IllegalStateException("容器进程退出等待失败：${it.message}", it) }
        if (privilegedSnapshot != null) { refreshPrivileged(true); return }
        val pending = java.util.ArrayDeque<Long>(livePids())
        val seen = mutableSetOf<Long>()
        while (pending.isNotEmpty()) {
            val parent = pending.removeFirst()
            if (!seen.add(parent)) continue
            File("/proc/$parent/task").listFiles().orEmpty().forEach { task ->
                val children = try { File(task, "children").readText() } catch (_: Exception) { "" }
                children.trim().split(Regex("\\s+")).mapNotNull(String::toLongOrNull).forEach childLoop@ { child ->
                    if (!ownsPid(parent)) return@childLoop
                    val stat = readStat(child) ?: return@childLoop
                    if (stat.parent != parent || child == pid) return@childLoop
                    identities.putIfAbsent(child, stat.start)
                    if (ownsPid(child)) pending.add(child)
                }
            }
        }
    }

    private fun refreshPrivileged(force: Boolean) {
        val inspector = privilegedSnapshot ?: return
        val now = System.nanoTime()
        if (!force && now - snapshotTime < 150_000_000) return
        val snapshot = inspector(identities.keys.toList()).mapNotNull(::parseStat).associateBy { it.pid }
        val accepted = identities.entries.filter { (pid, start) -> snapshot[pid]?.start == start }.map { it.key }.toMutableSet()
        var changed: Boolean
        do {
            changed = false
            snapshot.values.forEach { stat ->
                if (stat.parent in accepted && accepted.add(stat.pid)) { identities[stat.pid] = stat.start; changed = true }
            }
        } while (changed)
        rootLive = accepted.filter { snapshot[it]?.state != "Z" }
        snapshotTime = now
    }

    @Synchronized fun livePids(): List<Long> {
        if (privilegedSnapshot != null) { refreshPrivileged(false); return rootLive }
        var liveUnverifiedRoot: Long? = null
        unverifiedRoot?.let { root ->
            if (originalIsAlive()) {
                val stat = readStat(root)
                if (stat == null) {
                    if (originalIsAlive()) liveUnverifiedRoot = root
                } else if (stat.state != "Z" && confirmOriginalIsAlive()) {
                    identities[root] = stat.start
                    unverifiedRoot = null
                }
            } else unverifiedRoot = null
        }
        val live = identities.keys.filter(::ownsPid).toMutableList()
        liveUnverifiedRoot?.let(live::add)
        return live
    }
    @Synchronized fun liveIdentities(): List<Pair<Long, String>> = livePids().mapNotNull { pid -> identities[pid]?.let { pid to it } }
    fun hasLiveProcesses(): Boolean = originalIsAlive() || livePids().isNotEmpty()

    companion object {
        // Blocking exit waits must not occupy the shared IO slots used by command readers.
        private val exitDispatcher = Dispatchers.IO.limitedParallelism(Int.MAX_VALUE)
        private data class ProcStat(val pid: Long, val parent: Long, val start: String, val state: String, val group: Long, val session: Long)
        private fun parseStat(text: String): ProcStat? {
            val pid = text.substringBefore(' ').toLongOrNull() ?: return null
            val fields = text.substringAfterLast(") ").split(' ')
            val parent = fields.getOrNull(1)?.toLongOrNull() ?: return null
            val start = fields.getOrNull(19)?.takeIf { it.toLongOrNull() != null } ?: return null
            val group = fields.getOrNull(2)?.toLongOrNull() ?: return null
            val session = fields.getOrNull(3)?.toLongOrNull() ?: return null
            return ProcStat(pid, parent, start, fields[0], group, session)
        }
        fun handshake(file: File): Pair<Long, String>? = try {
            val lines = file.readLines()
            val pid = lines.firstOrNull()?.toLongOrNull()
            val stat = lines.getOrNull(1)?.let(::parseStat)
            if (pid != null && pid > 0 && stat != null && stat.pid == pid) pid to stat.start else null
        } catch (_: Exception) { null }
        private fun findPid(process: Process): Long? {
            try { return (Process::class.java.getMethod("pid").invoke(process) as Number).toLong() } catch (_: Exception) { }
            Regex("(?:pid=|pid: ?)([0-9]+)").find(process.toString())?.groupValues?.get(1)?.toLongOrNull()?.let { return it }
            return try { process.javaClass.getDeclaredField("pid").apply { isAccessible = true }.getInt(process).toLong() } catch (_: Exception) { null }
        }
    }
}
