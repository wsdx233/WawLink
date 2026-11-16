package dev.wawlink.runtime

import java.io.File
import java.io.RandomAccessFile
import java.nio.file.AccessDeniedException
import java.nio.file.Files
import java.nio.file.LinkOption.NOFOLLOW_LINKS
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class LocalContainerRuntimeTest {
    private fun fixture(block: (File, File, ContainerConfig) -> Unit) {
        val directory = Files.createTempDirectory("wawlink-local-runtime-test").toFile()
        val data = File(directory, "data").apply { mkdirs() }
        val config = root(directory, "source", "original")
        try { block(directory, data, config) } finally { RootfsArchives.deleteTree(directory) }
    }

    private fun root(directory: File, id: String, payload: String): ContainerConfig {
        val root = File(directory, id).apply { mkdirs() }
        File(root, "etc").mkdirs()
        File(root, "bin").mkdirs()
        // Only root validation runs: no test launches this ELF header or an engine substitute.
        File(root, "bin/sh").apply {
            writeBytes(ByteArray(20).apply {
                this[0] = 0x7f; this[1] = 'E'.code.toByte(); this[2] = 'L'.code.toByte(); this[3] = 'F'.code.toByte()
                this[5] = 1; this[18] = 62
            })
            setExecutable(true)
        }
        File(root, "etc/passwd").writeText("root:x:0:0::/root:/bin/sh\n")
        File(root, "payload").writeText(payload)
        return ContainerConfig(id, id, root.canonicalPath, arch = "amd64", shell = "/bin/sh", link2symlink = false, isDefault = true)
    }

    private fun saved(data: File, config: ContainerConfig, cached: Long?) {
        File(data, "containers.json").writeText(buildJsonObject {
            put("containers", Json.encodeToJsonElement(listOf(config)))
            if (cached != null) put("sizes", buildJsonObject { put(config.id, cached) })
        }.toString())
    }

    private class SizeGate {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        private val pending = AtomicBoolean(true)
        fun measure(root: File, excluded: Set<String>): Long {
            val actual = RootfsArchives.size(root, excluded)
            if (pending.compareAndSet(true, false)) {
                entered.countDown()
                check(release.await(10, TimeUnit.SECONDS)) { "Size calculation was not released" }
            }
            return actual
        }
        suspend fun awaitEntry() = withContext(Dispatchers.IO) {
            check(entered.await(10, TimeUnit.SECONDS)) { "Background size calculation did not start" }
        }
    }

    // Synchronize completion only; assertions below concern public state and persisted data.
    private suspend fun awaitSizeWork(runtime: LocalContainerRuntime) = withTimeout(10_000) {
        val field = LocalContainerRuntime::class.java.getDeclaredField("scope").apply { isAccessible = true }
        val scope = field.get(runtime) as CoroutineScope
        while (true) {
            val jobs = scope.coroutineContext[Job]!!.children.toList()
            if (jobs.isEmpty()) break
            jobs.joinAll()
        }
    }

    private fun startup(cached: Long?) = fixture { _, data, config -> runBlocking {
        saved(data, config, cached)
        val gate = SizeGate()
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"), gate::measure)
        try {
            gate.awaitEntry()
            val initial = runtime.containers.value.single()
            assertEquals(cached ?: 0L, initial.sizeBytes)
            assertFalse(initial.running)
            assertEquals(0L, initial.runGeneration)
            gate.release.countDown()
            val actual = RootfsArchives.size(File(config.rootfsPath))
            withTimeout(10_000) { runtime.containers.first { it.single().sizeBytes == actual } }
            val persisted = Json.parseToJsonElement(File(data, "containers.json").readText()).jsonObject
            assertEquals(actual, persisted.getValue("sizes").jsonObject.getValue(config.id).jsonPrimitive.long)
        } finally { gate.release.countDown(); runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun cachedFirstScreenDoesNotWaitForRecursiveSizeCalculation() = startup(987_654L)

    @Test fun oldConfigurationWithoutASizeCacheLoadsAndRefreshesOffline() = startup(null)

    @Test fun startupResidualCleanupFailurePublishesErrorAndRetainsRetryableIdentities() = fixture { _, data, config -> runBlocking {
        saved(data, config, 111L)
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        val exited = ProcessBuilder("/bin/sh", "-c", "exit 0").start()
        exited.waitFor()
        var refuseSnapshot = false
        var refusedReads = 0
        val tracked = TrackedProcess(exited, keeper = false, ownerScope = this, rootIdentity = 1L to "1",
            privilegedSnapshot = {
                if (refuseSnapshot) { refusedReads++; error("residual identity access refused") }
                emptyList()
            })
        val field = LocalContainerRuntime::class.java.getDeclaredField("processes").apply { isAccessible = true }
        @Suppress("UNCHECKED_CAST")
        val records = field.get(runtime) as MutableMap<String, MutableList<TrackedProcess>>
        records[config.id] = mutableListOf(tracked)
        refuseSnapshot = true
        try {
            val failure = assertFailsWith<IllegalStateException> { runtime.start(config.id) }
            assertTrue(failure.message.orEmpty().contains("residual identity access refused"))
            assertEquals(1, refusedReads, "Startup must not immediately repeat the same failed residual cleanup")
            val state = runtime.containers.value.single()
            assertTrue(state.error.orEmpty().contains("residual identity access refused"))
            assertFalse(state.running)
            assertEquals(0L, state.runGeneration)
            assertEquals(0L, state.stopGeneration, "Failed startup cleanup is not a new user stop epoch")
            assertSame(tracked, records.getValue(config.id).single())
            refuseSnapshot = false
            runtime.stop(config.id)
            assertFalse(records.containsKey(config.id))
            assertEquals(1L, runtime.containers.value.single().stopGeneration)
            assertEquals(null, runtime.containers.value.single().error)
        } finally { refuseSnapshot = false; runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun idleStopConsumesCleanupFailureAndRejectsStaleEpochsAndKeepers() = fixture { _, data, config -> runBlocking {
        saved(data, config.copy(autoSleep = true), 111L)
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        val exited = ProcessBuilder("/bin/sh", "-c", "exit 0").start()
        exited.waitFor()
        var refuseSnapshot = false
        var deniedReads = 0
        val keeper = TrackedProcess(exited, keeper = true, ownerScope = this, rootIdentity = 1L to "1", privilegedSnapshot = {
            if (refuseSnapshot) { deniedReads++; error("idle snapshot refused") }
            emptyList()
        })
        val oldKeeper = TrackedProcess(exited, keeper = true, ownerScope = this, rootIdentity = 2L to "2", privilegedSnapshot = { emptyList() })
        @Suppress("UNCHECKED_CAST")
        fun <T> records(name: String): MutableMap<String, T> =
            LocalContainerRuntime::class.java.getDeclaredField(name).apply { isAccessible = true }.get(runtime) as MutableMap<String, T>
        val processes = records<MutableList<TrackedProcess>>("processes")
        val keepers = records<TrackedProcess>("keepers")
        val stops = records<Long>("stopGenerations")
        val runs = records<Long>("runGenerations")
        processes[config.id] = mutableListOf(keeper)
        keepers[config.id] = keeper
        stops[config.id] = 2L
        runs[config.id] = 7L
        val idleStop = LocalContainerRuntime::class.java.getDeclaredMethod("stopIdleLocked", String::class.java,
            java.lang.Long::class.java, TrackedProcess::class.java).apply { isAccessible = true }
        refuseSnapshot = true
        try {
            idleStop.invoke(runtime, config.id, 1L, keeper)
            idleStop.invoke(runtime, config.id, 2L, oldKeeper)
            assertEquals(0, deniedReads)
            assertFalse(keeper.stopping, "Stale timers must not stop the current keeper")
            assertEquals(2L, stops[config.id])
            idleStop.invoke(runtime, config.id, 2L, keeper) // Must return normally, not escape scope.launch.
            assertEquals(1, deniedReads)
            assertTrue(runtime.containers.value.single().error.orEmpty().contains("idle snapshot refused"))
            assertEquals(3L, runtime.containers.value.single().stopGeneration)
            assertEquals(7L, runtime.containers.value.single().runGeneration)
            assertSame(keeper, keepers[config.id])
            assertSame(keeper, processes.getValue(config.id).single())
            refuseSnapshot = false
            runtime.stop(config.id)
            assertEquals(null, runtime.containers.value.single().error)
            assertFalse(processes.containsKey(config.id))
        } finally { refuseSnapshot = false; runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun closeProcessReclaimsOnlyItsTreeWithoutHoldingTheRuntimeMutex(): Unit = fixture { _, data, config -> runBlocking {
        saved(data, config, 111L)
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        val command = ProcessBuilder("/bin/sh", "-c", "sleep 60 & echo \$!; wait").start()
        val child = command.inputStream.bufferedReader().readLine().toLong()
        val sibling = ProcessBuilder("sleep", "60").start()
        val keeperProcess = ProcessBuilder("sleep", "60").start()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val pauseSnapshot = AtomicBoolean(false)
        val target = TrackedProcess(command, keeper = false, ownerScope = this, statReader = { pid ->
            if (pauseSnapshot.compareAndSet(true, false)) {
                entered.countDown()
                check(release.await(5, TimeUnit.SECONDS)) { "Snapshot was not released" }
            }
            runCatching { File("/proc/$pid/stat").readText() }.getOrNull()
        })
        val siblingRecord = TrackedProcess(sibling, keeper = false, ownerScope = this)
        val keeper = TrackedProcess(keeperProcess, keeper = true, ownerScope = this)
        @Suppress("UNCHECKED_CAST")
        val records = LocalContainerRuntime::class.java.getDeclaredField("processes").apply { isAccessible = true }
            .get(runtime) as MutableMap<String, MutableList<TrackedProcess>>
        @Suppress("UNCHECKED_CAST")
        val keepers = LocalContainerRuntime::class.java.getDeclaredField("keepers").apply { isAccessible = true }
            .get(runtime) as MutableMap<String, TrackedProcess>
        records[config.id] = mutableListOf(target, siblingRecord, keeper)
        keepers[config.id] = keeper
        try {
            pauseSnapshot.set(true)
            val closing = async(Dispatchers.IO) { runtime.closeProcess(config.id, command) }
            assertTrue(withContext(Dispatchers.IO) { entered.await(5, TimeUnit.SECONDS) })
            // The target's slow /proc read must not block unrelated metadata/container work.
            withTimeout(1_000) { runtime.readMetadata(config.id) }
            release.countDown()
            closing.await()
            assertFalse(target.hasLiveProcesses())
            val childStat = runCatching { File("/proc/$child/stat").readText() }.getOrNull()
            assertTrue(childStat == null || childStat.substringAfterLast(") ").startsWith("Z "))
            assertTrue(sibling.isAlive)
            assertTrue(keeperProcess.isAlive)
            assertFalse(siblingRecord.stopping)
            assertFalse(keeper.stopping)
            assertEquals<List<TrackedProcess>>(listOf(siblingRecord, keeper), records.getValue(config.id).toList())
            assertEquals(0L, runtime.containers.value.single().stopGeneration)
            runtime.closeProcess(config.id, command) // Repeated close is harmless after removal.
        } finally {
            release.countDown()
            try { runtime.close(); awaitSizeWork(runtime) }
            finally {
                listOf(command, sibling, keeperProcess).forEach { if (it.isAlive) it.destroyForcibly() }
                HostCommands.run(listOf("/bin/sh", "-c", "kill -KILL $child 2>/dev/null || true"))
            }
        }
    } }

    @Test fun closeProcessPreservesPrivilegedIdentityBeforeDestroyingItsLauncherAndCanRetry(): Unit = fixture { _, data, config -> runBlocking {
        saved(data, config.copy(engine = ContainerEngineType.CHROOT), 111L)
        val signals = java.util.Collections.synchronizedList(mutableListOf<String>())
        // Only Root authorization is simulated. Signals, launcher and worker lifetimes are real.
        val runtime = LocalContainerRuntime(RuntimePaths(data), RootfsArchives::size, runRootCommand = { command ->
            if (command.contains("kill -TERM") || command.contains("kill -KILL")) signals += command
            HostCommands.run(listOf("/bin/sh", "-c", "id() { printf '0\\n'; };\n$command"))
        })
        val launcher = ProcessBuilder("sleep", "60").start()
        val worker = ProcessBuilder("/bin/sh", "-c", "sleep 60 & echo \$!; wait").start()
        val child = worker.inputStream.bufferedReader().readLine().toLong()
        val sibling = ProcessBuilder("sleep", "60").start()
        fun stat(pid: Long): String? = runCatching { File("/proc/$pid/stat").readText() }.getOrNull()
        val identity = requireNotNull(stat(worker.pid())).substringAfterLast(") ").split(' ')[19]
        var refuseSnapshot = false
        val target = TrackedProcess(launcher, keeper = false, ownerScope = this, rootIdentity = worker.pid() to identity, privilegedSnapshot = {
            check(!refuseSnapshot) { "Root snapshot refused" }
            listOfNotNull(stat(worker.pid()), stat(child))
        })
        val siblingRecord = TrackedProcess(sibling, keeper = false, ownerScope = this)
        @Suppress("UNCHECKED_CAST")
        val records = LocalContainerRuntime::class.java.getDeclaredField("processes").apply { isAccessible = true }
            .get(runtime) as MutableMap<String, MutableList<TrackedProcess>>
        records[config.id] = mutableListOf(target, siblingRecord)
        try {
            refuseSnapshot = true
            assertEquals("Root snapshot refused", assertFailsWith<IllegalStateException> {
                runtime.closeProcess(config.id, launcher)
            }.message)
            assertTrue(launcher.isAlive, "A failed identity read must preserve the original launcher")
            assertTrue(worker.isAlive)
            assertSame(target, records.getValue(config.id).first())
            assertTrue(signals.isEmpty())
            refuseSnapshot = false
            runtime.closeProcess(config.id, launcher)
            assertFalse(launcher.isAlive)
            assertFalse(target.hasLiveProcesses())
            assertTrue(sibling.isAlive)
            assertEquals<List<TrackedProcess>>(listOf(siblingRecord), records.getValue(config.id).toList())
            assertTrue(signals.any { it.contains("/proc/${worker.pid()}/stat") })
            assertTrue(signals.none { it.contains("/proc/${sibling.pid()}/stat") })
            assertEquals(0L, runtime.containers.value.single().stopGeneration)
        } finally {
            refuseSnapshot = false
            try { runtime.close(); awaitSizeWork(runtime) }
            finally {
                listOf(launcher, worker, sibling).forEach { if (it.isAlive) it.destroyForcibly() }
                HostCommands.run(listOf("/bin/sh", "-c", "kill -KILL $child 2>/dev/null || true"))
            }
        }
    } }

    @Test fun closeProcessRejectsKeepersAndCommandsOwnedByAnotherContainer(): Unit = fixture { _, data, config -> runBlocking {
        saved(data, config, 111L)
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        val command = ProcessBuilder("sleep", "60").start()
        val keeperProcess = ProcessBuilder("sleep", "60").start()
        val target = TrackedProcess(command, keeper = false, ownerScope = this)
        val keeper = TrackedProcess(keeperProcess, keeper = true, ownerScope = this)
        @Suppress("UNCHECKED_CAST")
        val records = LocalContainerRuntime::class.java.getDeclaredField("processes").apply { isAccessible = true }
            .get(runtime) as MutableMap<String, MutableList<TrackedProcess>>
        records[config.id] = mutableListOf(target, keeper)
        try {
            assertFailsWith<IllegalStateException> { runtime.closeProcess("another-container", command) }
            assertFailsWith<IllegalStateException> { runtime.closeProcess(config.id, keeperProcess) }
            assertTrue(command.isAlive)
            assertTrue(keeperProcess.isAlive)
            assertFalse(target.stopping)
            assertFalse(keeper.stopping)
            assertEquals(0L, runtime.containers.value.single().stopGeneration)
        } finally {
            try { runtime.close(); awaitSizeWork(runtime) }
            finally { listOf(command, keeperProcess).forEach { if (it.isAlive) it.destroyForcibly() } }
        }
    } }

    private fun restrictedStartup(engine: ContainerEngineType, pending: Boolean = false) = fixture { directory, data, config -> runBlocking {
        val root = File(config.rootfsPath)
        val home = File(root, "srv/private-home")
        File(root, "etc/passwd").writeText("root:x:0:0::/root:/bin/sh\nalice:x:1001:1002::/srv/private-home:/bin/sh\n")
        val document = File(home, ".wawlink/container.json").apply {
            parentFile.mkdirs()
            writeText("""{"title":"Read from the guest","description":"cold startup","version":"13"}""")
        }
        Files.setPosixFilePermissions(home.toPath(), PosixFilePermissions.fromString("rwx------"))
        Files.setPosixFilePermissions(document.toPath(), PosixFilePermissions.fromString("rw-------"))
        val active = config.copy(engine = engine, user = "alice")
        if (engine == ContainerEngineType.PROOT) RootfsOwnership.save(root, OwnershipManifest(
            entries = emptyMap(), physical = ContainerEngineType.CHROOT,
            pending = if (pending) OwnershipMigration(ContainerEngineType.CHROOT, ContainerEngineType.PROOT) else null,
        ))
        saved(data, active, 111L)
        RestrictedRootShell(directory, root).use { shell ->
            val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"),
                RootfsArchives::size, runRootCommand = shell::run)
            try {
                val initial = runtime.containers.value.single()
                assertEquals(null, initial.error)
                assertEquals(null, initial.metadataError)
                assertEquals(ContainerMetadata("Read from the guest", "cold startup", "13"), initial.config.metadata)
                assertEquals(engine, initial.config.engine)
                assertFalse(initial.running)
                assertEquals(0L, initial.runGeneration)
                val snapshot = runtime.readMetadata(active.id)
                assertEquals(initial.config.metadata, snapshot.value)
                assertTrue(snapshot.revision != null)
                assertEquals(0, shell.mode(root))
                shell.inspect {
                    assertEquals(448, shell.mode(home))
                    assertEquals(384, shell.mode(document))
                    assertTrue(document.readText().contains("Read from the guest"))
                }
            } finally { runtime.close(); awaitSizeWork(runtime) }
        }
    } }

    @Test fun savedChrootLoadsActualMetadataThroughAnAppInaccessibleRoot() = restrictedStartup(ContainerEngineType.CHROOT)

    @Test fun savedProotWithPhysicalChrootLoadsActualMetadataWithoutFalseStructureErrors() = restrictedStartup(ContainerEngineType.PROOT)

    @Test fun savedProotWithPendingChrootMigrationLoadsActualMetadataOffline() = restrictedStartup(ContainerEngineType.PROOT, pending = true)

    @Test fun coldStartupRetainsRootRefusalInsteadOfReportingAnEmptyMetadataSuccess() = fixture { directory, data, config -> runBlocking {
        val root = File(config.rootfsPath)
        val document = File(root, "root/.wawlink/container.json").apply {
            parentFile.mkdirs(); writeText("""{"title":"Must not disappear"}""")
        }
        saved(data, config.copy(engine = ContainerEngineType.CHROOT), 111L)
        RestrictedRootShell(directory, root).use { shell ->
            val refusal = IllegalStateException("Root authorization refused by test host")
            val runtime = LocalContainerRuntime(RuntimePaths(data), RootfsArchives::size, runRootCommand = { throw refusal })
            try {
                val initial = runtime.containers.value.single()
                assertTrue(initial.error.orEmpty().contains(refusal.message!!))
                assertTrue(initial.metadataError.orEmpty().contains(refusal.message!!))
                val failure = assertFailsWith<IllegalStateException> { runtime.readMetadata(config.id) }
                assertTrue(failure.message.orEmpty().contains(refusal.message!!))
                assertFalse(runtime.containers.value.single().running)
                assertEquals(0, shell.mode(root))
                shell.inspect { assertEquals("""{"title":"Must not disappear"}""", document.readText()) }
            } finally { runtime.close(); awaitSizeWork(runtime) }
        }
    } }

    @Test fun coldStartupRetainsARealMissingShellBehindTheRestrictedRoot() = fixture { directory, data, config -> runBlocking {
        val root = File(config.rootfsPath)
        assertTrue(File(root, "bin/sh").delete())
        saved(data, config.copy(engine = ContainerEngineType.CHROOT), 111L)
        RestrictedRootShell(directory, root).use { shell ->
            val runtime = LocalContainerRuntime(RuntimePaths(data), RootfsArchives::size, runRootCommand = shell::run)
            try {
                val initial = runtime.containers.value.single()
                assertTrue(initial.error.orEmpty().contains("bin/sh"))
                assertTrue(initial.error.orEmpty().contains("No such file"))
                assertTrue(initial.metadataError.orEmpty().contains("bin/sh"))
                val failure = assertFailsWith<IllegalStateException> { runtime.readMetadata(config.id) }
                assertTrue(failure.message.orEmpty().contains("bin/sh"))
                assertEquals(0, shell.mode(root))
                shell.inspect { assertFalse(File(root, "bin/sh").exists()) }
            } finally { runtime.close(); awaitSizeWork(runtime) }
        }
    } }

    @Test fun chrootSizeCountsLogicalBytesPerPathAndPrunesMountsWithoutFollowingLinks() = fixture { directory, data, config -> runBlocking {
        val root = File(config.rootfsPath)
        var expected = File(root, "bin/sh").length() + File(root, "etc/passwd").length() + File(root, "payload").length()
        val sparse = File(root, "sparse")
        RandomAccessFile(sparse, "rw").use { it.setLength(4L * 1024 * 1024 * 1024 + 7) }
        Files.createLink(File(root, "hard link\nwith newline").toPath(), sparse.toPath())
        expected += sparse.length() * 2
        repeat(300) { index ->
            val file = File(root, "many/file-$index").apply { parentFile.mkdirs(); writeText("payload-$index") }
            expected += file.length()
        }
        val outside = File(directory, "outside").apply { mkdir() }
        File(outside, "large").writeBytes(ByteArray(8192))
        Files.createSymbolicLink(File(root, "outside-link").toPath(), outside.toPath())
        Files.createSymbolicLink(File(root, "inside-link").toPath(), sparse.toPath())
        val configuredMount = "mnt/config [*]?\\path"
        val actualMount = "mnt/actual mount"
        for (name in listOf(configuredMount, actualMount, "proc", "sys", "dev", "host-rootfs", "sdcard", "storage")) {
            File(root, "$name/ignored").apply { parentFile.mkdirs(); writeBytes(ByteArray(4096)) }
        }
        val neighbor = File(root, "mnt/config [*]?\\path-neighbor/kept").apply { parentFile.mkdirs(); writeText("keep") }
        expected += neighbor.length()
        val privileged = config.copy(engine = ContainerEngineType.CHROOT, mounts = listOf(ContainerMount(outside.path, "/$configuredMount")))
        saved(data, privileged, 111L)
        val mountPath = File(root, actualMount).path.replace(" ", "\\040")
        val runtime = LocalContainerRuntime(RuntimePaths(data), RootfsArchives::size, runRootCommand = { command ->
            if (command == "cat /proc/self/mountinfo") "1 0 0:1 / $mountPath rw - tmpfs tmpfs rw\n"
            else HostCommands.run(listOf("/bin/sh", "-c", "id() { printf '0\\n'; };\n$command"))
        })
        try {
            awaitSizeWork(runtime)
            assertEquals(expected, runtime.containers.value.single().sizeBytes)
            val persisted = Json.parseToJsonElement(File(data, "containers.json").readText()).jsonObject
            assertEquals(expected, persisted.getValue("sizes").jsonObject.getValue(config.id).jsonPrimitive.long)
        } finally { runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun mappedSizeExcludesTheMountPathRatherThanItsSymlinkDestination() = fixture { _, data, config -> runBlocking {
        val root = File(config.rootfsPath)
        val kept = File(root, "real-directory/kept").apply { parentFile.mkdirs(); writeText("must remain counted") }
        Files.createSymbolicLink(File(root, "mounted-alias").toPath(), kept.parentFile.toPath())
        for (name in listOf("sdcard", "storage", "proc", "sys", "dev", "host-rootfs")) {
            File(root, "$name/ignored").apply { parentFile.mkdirs(); writeText("not guest data") }
        }
        val expected = File(root, "bin/sh").length() + File(root, "etc/passwd").length() + File(root, "payload").length() + kept.length()
        saved(data, config.copy(mounts = listOf(ContainerMount("/outside", "/mounted-alias"))), 111L)
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        try {
            awaitSizeWork(runtime)
            assertEquals(expected, runtime.containers.value.single().sizeBytes)
        } finally { runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun failedSizeProducerOrAnEarlierBatchCannotReplaceTheSuccessfulCache() {
        for (broken in listOf("find", "stat")) fixture { directory, data, config -> runBlocking {
            val root = File(config.rootfsPath)
            repeat(300) { File(root, "file-$it").writeText("payload") }
            val tools = File(directory, "tools").apply { mkdir() }
            File(tools, broken).apply {
                writeText(if (broken == "find") "#!/bin/sh\n/usr/bin/find \"\$@\"\nexit 37\n"
                    else "#!/bin/sh\nfor path do [ \"\$path\" != './payload' ] || exit 37; done\nexec /usr/bin/stat \"\$@\"\n")
                setExecutable(true)
            }
            saved(data, config.copy(engine = ContainerEngineType.CHROOT), 111L)
            val runtime = LocalContainerRuntime(RuntimePaths(data), RootfsArchives::size, runRootCommand = { command ->
                HostCommands.run(ProcessBuilder("/bin/sh", "-c", command).apply {
                    environment()["PATH"] = tools.path + ":" + environment()["PATH"]
                })
            })
            try {
                awaitSizeWork(runtime)
                assertEquals(111L, runtime.containers.value.single().sizeBytes, "$broken must not publish a partial sum")
                val persisted = Json.parseToJsonElement(File(data, "containers.json").readText()).jsonObject
                assertEquals(111L, persisted.getValue("sizes").jsonObject.getValue(config.id).jsonPrimitive.long)
            } finally { runtime.close(); awaitSizeWork(runtime) }
        } }
    }

    @Test fun lateSizeCannotReplaceADeletedAndReregisteredContainer() = fixture { directory, data, config -> runBlocking {
        saved(data, config, 111L)
        val gate = SizeGate()
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"), gate::measure)
        try {
            gate.awaitEntry()
            runtime.delete(config.id)
            assertTrue(runtime.containers.value.isEmpty())
            val replacement = root(directory, "replacement", "a different rootfs with a different size").copy(id = config.id)
            runtime.register(replacement)
            val actual = RootfsArchives.size(File(replacement.rootfsPath))
            withTimeout(10_000) { runtime.containers.first { it.single().sizeBytes == actual } }
            gate.release.countDown()
            awaitSizeWork(runtime)
            assertEquals(replacement.rootfsPath, runtime.containers.value.single().config.rootfsPath)
            assertEquals(actual, runtime.containers.value.single().sizeBytes)
        } finally { gate.release.countDown(); runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun changedMetadataInvalidatesThePendingConfigurationSnapshot() = fixture { _, data, config -> runBlocking {
        saved(data, config, 111L)
        val gate = SizeGate()
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"), gate::measure)
        try {
            gate.awaitEntry()
            runtime.saveMetadata(config.id, ContainerMetadata("New portable title"), null)
            val actual = RootfsArchives.size(File(config.rootfsPath))
            gate.release.countDown()
            awaitSizeWork(runtime)
            assertEquals("New portable title", runtime.containers.value.single().config.displayName)
            assertEquals(actual, runtime.containers.value.single().sizeBytes)
        } finally { gate.release.countDown(); runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun lateSizeCannotPublishAfterClose() = fixture { _, data, config -> runBlocking {
        saved(data, config, 111L)
        val gate = SizeGate()
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"), gate::measure)
        try {
            gate.awaitEntry()
            withTimeout(10_000) { withContext(Dispatchers.IO) { runtime.close() } }
            val closed = runtime.containers.value
            gate.release.countDown()
            awaitSizeWork(runtime)
            assertEquals(closed, runtime.containers.value)
            assertEquals(111L, runtime.containers.value.single().sizeBytes)
        } finally { gate.release.countDown(); runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun stoppedRootfsRejectsSameContainerWorkButDoesNotHoldTheGlobalMutex() = fixture { directory, data, config -> runBlocking {
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        try {
            runtime.register(config.copy(isDefault = false))
            val other = root(directory, "other", "other").copy(isDefault = true)
            runtime.register(other)
            val operation = async {
                runtime.withStoppedRootfs(config.id, "reserved-clone") { entered.complete(Unit); release.await() }
            }
            entered.await()
            withTimeout(10_000) {
                runtime.stop(other.id)
                runtime.saveMetadata(other.id, ContainerMetadata("Other remains editable"), null)
                runtime.setDefault(other.id)
                val third = root(directory, "third", "third").copy(isDefault = false)
                runtime.register(third)
                runtime.delete(third.id)
            }
            val rejected = listOf<suspend () -> Unit>(
                { runtime.start(config.id) },
                { runtime.update(config) },
                { runtime.delete(config.id) },
                { runtime.setDefault(config.id) },
                { runtime.readMetadata(config.id) },
                { runtime.saveMetadata(config.id, ContainerMetadata("Forbidden"), null) },
                { runtime.clone(config.id, "Forbidden") },
                { runtime.exportRootfs(config.id, File(directory, "busy.tar.zst").path) },
                { runtime.openProcess(config.id, listOf("/bin/true")) },
                { runtime.openRunningProcess(config.id, listOf("/bin/true")) },
                { runtime.openTerminal(config.id) },
                { runtime.register(root(directory, "reserved-clone", "reserved").copy(isDefault = false)) },
            )
            rejected.forEach { call ->
                val failure = assertFailsWith<IllegalStateException> { withTimeout(10_000) { call() } }
                assertTrue(failure.message.orEmpty().contains("正在归档"))
            }
            val generation = runtime.containers.value.first { it.config.id == config.id }.stopGeneration
            runtime.stop(config.id)
            release.complete(Unit)
            operation.await()
            val stopped = runtime.containers.value.first { it.config.id == config.id }
            assertFalse(stopped.running)
            assertEquals(generation + 1, stopped.stopGeneration)
            assertEquals("Other remains editable", runtime.readMetadata(other.id).value.title)
            runtime.readMetadata(config.id) // The lease was released without restoring an old config.
        } finally { release.complete(Unit); runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun cancellationAndFailureReleaseTheLeaseAndPreserveTheOriginalFailure() = fixture { _, data, config -> runBlocking {
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        try {
            runtime.register(config)
            val entered = CompletableDeferred<Unit>()
            val operation = async {
                runtime.withStoppedRootfs(config.id) { entered.complete(Unit); awaitCancellation() }
            }
            entered.await()
            operation.cancelAndJoin()
            runtime.readMetadata(config.id)
            val original = IllegalArgumentException("original archive failure")
            val failure = assertFails { runtime.withStoppedRootfs(config.id) { throw original } }
            assertSame(original, failure)
            runtime.readMetadata(config.id)
            assertFalse(runtime.containers.value.single().running)
            assertEquals(0L, runtime.containers.value.single().runGeneration)
        } finally { runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun closeDoesNotWaitForTheArchiveActionOrReopenTheContainer() = fixture { _, data, config -> runBlocking {
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        try {
            runtime.register(config)
            val operation = async {
                runtime.withStoppedRootfs(config.id) {
                    entered.complete(Unit)
                    withContext(NonCancellable) { release.await() }
                }
            }
            entered.await()
            withTimeout(10_000) { withContext(Dispatchers.IO) { runtime.close() } }
            val closed = runtime.containers.value
            release.complete(Unit)
            operation.await()
            assertEquals(closed, runtime.containers.value)
            assertFailsWith<IllegalStateException> { runtime.start(config.id) }
        } finally { release.complete(Unit); runtime.close(); awaitSizeWork(runtime) }
    } }

    private fun interruptedClone(close: Boolean) = fixture { directory, data, config -> runBlocking {
        val gate = SizeGate()
        val managedRoots = File(data, "rootfs").canonicalFile.toPath()
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched")) { root, excluded ->
            if (root.toPath().startsWith(managedRoots)) gate.measure(root, excluded) else RootfsArchives.size(root, excluded)
        }
        try {
            runtime.register(config)
            val other = root(directory, "other", "other").copy(isDefault = false)
            runtime.register(other)
            val cloning = async(Dispatchers.IO) { runCatching { runtime.clone(config.id, "Interrupted clone") } }
            gate.awaitEntry() // Actual export and extraction completed; clone size IO is still pending.
            withTimeout(10_000) {
                runtime.stop(other.id)
                runtime.saveMetadata(other.id, ContainerMetadata("Independent edit"), null)
            }
            assertFailsWith<IllegalStateException> { runtime.start(config.id) }
            if (close) {
                withTimeout(10_000) { withContext(Dispatchers.IO) { runtime.close() } }
                gate.release.countDown()
                assertTrue(cloning.await().isFailure)
            } else {
                val generation = runtime.containers.value.first { it.config.id == config.id }.stopGeneration
                runtime.stop(config.id)
                cloning.cancel()
                gate.release.countDown()
                cloning.cancelAndJoin()
                assertEquals(generation + 1, runtime.containers.value.first { it.config.id == config.id }.stopGeneration)
                assertEquals("Independent edit", runtime.readMetadata(other.id).value.title)
                runtime.readMetadata(config.id)
            }
            assertEquals(setOf(config.id, other.id), runtime.containers.value.map { it.config.id }.toSet())
            assertTrue(runtime.containers.value.all { !it.running && it.runGeneration == 0L })
            assertTrue(File(data, "rootfs").listFiles().orEmpty().isEmpty())
            assertTrue(File(data, "downloads").listFiles().orEmpty().isEmpty())
        } finally { gate.release.countDown(); runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun cancellingAnActualCloneCleansItsFilesAndReleasesAdmission() = interruptedClone(close = false)

    @Test fun closingDuringAnActualCloneRejectsLateRegistrationAndCleansItsFiles() = interruptedClone(close = true)

    @Test fun cloneRegistrationFailureRollsBackConfigurationSizeAndPartialFiles() = fixture { _, data, config -> runBlocking {
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        try {
            runtime.register(config)
            awaitSizeWork(runtime)
            val original = runtime.containers.value.single()
            val state = File(data, "containers.json")
            assertTrue(state.delete())
            assertTrue(state.mkdir())
            val sentinel = File(state, "keep").apply { writeText("nonempty directories cannot be replaced") }
            assertFails { runtime.clone(config.id, "Uncommitted clone") }
            assertEquals(listOf(config.id), runtime.containers.value.map { it.config.id })
            assertEquals(original.sizeBytes, runtime.containers.value.single().sizeBytes)
            assertTrue(File(data, "rootfs").listFiles().orEmpty().isEmpty())
            assertTrue(File(data, "downloads").listFiles().orEmpty().isEmpty())
            assertTrue(sentinel.delete())
            assertTrue(state.delete())
            runtime.setDefault(config.id)
            val persisted = Json.parseToJsonElement(state.readText()).jsonObject
            assertEquals(setOf(config.id), persisted.getValue("sizes").jsonObject.keys)
            assertEquals(1, persisted.getValue("containers").let { Json.decodeFromString<List<ContainerConfig>>(it.toString()) }.size)
            runtime.readMetadata(config.id)
        } finally { runtime.close(); awaitSizeWork(runtime) }
    } }

    @Test fun exportFailurePreservesItsCauseAndRemovesTheTemporaryArchive() = fixture { directory, data, config -> runBlocking {
        val runtime = LocalContainerRuntime(RuntimePaths(data, hostProot = "/missing-engine-must-not-be-launched"))
        try {
            runtime.register(config)
            val target = File(config.rootfsPath, "forbidden.tar.zst")
            val failure = assertFailsWith<IllegalStateException> { runtime.exportRootfs(config.id, target.path) }
            // Coroutine stacktrace recovery can insert a copy of the export wrapper;
            // the original archive rejection must still survive in its cause chain.
            val cause = generateSequence(failure.cause) { it.cause }.firstOrNull { it is IllegalArgumentException }
            assertTrue(cause is IllegalArgumentException, failure.stackTraceToString())
            assertFalse(target.exists())
            assertFalse(File(config.rootfsPath).listFiles().orEmpty().any { it.name.endsWith(".part") })
            runtime.readMetadata(config.id)
            val existing = File(directory, "existing.tar.zst").apply { writeText("keep existing bytes") }
            assertFails { runtime.exportRootfs(config.id, existing.path) }
            assertEquals("keep existing bytes", existing.readText())
        } finally { runtime.close(); awaitSizeWork(runtime) }
    } }
}

/**
 * Models only Root identity/traversal: each injected Root call opens owner traversal, executes
 * the production shell against real files, and restores mode 000. This is not privileged-kernel
 * acceptance. A Root JVM can bypass 000, so only non-Root JVMs assert actual App AccessDenied.
 */
internal class RestrictedRootShell(directory: File, private val root: File) : AutoCloseable {
    private val originalPermissions = Files.getPosixFilePermissions(root.toPath())
    private val tools = Files.createTempDirectory(directory.toPath(), "root-access-tools-").toFile()
    val appTraversalDenied: Boolean

    init {
        File(tools, "id").apply {
            writeText("#!/bin/sh\nif [ \"\$1\" = -u ]; then echo 0; else exec /usr/bin/id \"\$@\"; fi\n")
            setExecutable(true)
        }
        // Keep physical files owned by the test process, while exercising the real chown operation.
        File(tools, "chown").apply {
            writeText("#!/bin/sh\n[ \"\$1\" != -h ] || shift\nshift\nexec /usr/bin/chown -h '${ChrootHostIdentity.uid}:${ChrootHostIdentity.gid}' \"\$@\"\n")
            setExecutable(true)
        }
        Files.setPosixFilePermissions(root.toPath(), emptySet())
        appTraversalDenied = try {
            Files.readAttributes(File(root, ".").toPath(), BasicFileAttributes::class.java)
            false
        } catch (_: AccessDeniedException) { true }
        if (ChrootHostIdentity.uid != 0) assertTrue(appTraversalDenied, "mode 000 must deny this non-Root JVM traversal")
    }

    @Synchronized fun <T> inspect(block: () -> T): T {
        Files.setPosixFilePermissions(root.toPath(), PosixFilePermissions.fromString("rwx------"))
        return try { block() } finally { Files.setPosixFilePermissions(root.toPath(), emptySet()) }
    }

    fun run(command: String): String = inspect {
        HostCommands.run(ProcessBuilder("/bin/sh", "-c", command).apply {
            environment()["PATH"] = tools.path + ":" + environment()["PATH"]
            environment()["LC_ALL"] = "C"
        })
    }

    @Synchronized fun mode(file: File) = (Files.getAttribute(file.toPath(), "unix:mode", NOFOLLOW_LINKS) as Number).toInt() and 0xFFF

    override fun close() {
        Files.setPosixFilePermissions(root.toPath(), originalPermissions)
        RootfsArchives.deleteTree(tools)
    }
}
