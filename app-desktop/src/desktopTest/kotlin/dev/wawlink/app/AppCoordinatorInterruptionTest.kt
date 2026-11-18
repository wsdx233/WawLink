package dev.wawlink.app

import dev.wawlink.driver.OmpProcessPool
import dev.wawlink.runtime.ContainerConfig
import dev.wawlink.runtime.ContainerInfo
import dev.wawlink.runtime.ContainerRuntime
import dev.wawlink.runtime.LocalContainerRuntime
import dev.wawlink.runtime.RuntimePaths
import dev.wawlink.runtime.TerminalSession
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class AppCoordinatorInterruptionTest {
    private class FeatureFixture(debugFailure: Boolean = false) : AutoCloseable {
        val directory = Files.createTempDirectory("wawlink-feature-notice-").toFile()
        private val base = LocalContainerRuntime(RuntimePaths(directory.resolve("runtime")))
        private val home = directory.resolve("home").apply { mkdirs() }
        private val brokenBun = directory.resolve("bun").apply {
            writeText("#!/bin/sh\nprintf 'fchown: EPERM\\n' >&2\nexit 1\n")
            check(setExecutable(true))
        }
        private val children = mutableListOf<Process>()
        private val state = MutableStateFlow(listOf(ContainerInfo(
            ContainerConfig("guest", "测试容器", directory.resolve("root").path), running = true,
        )))
        private val runtime = object : ContainerRuntime by base {
            override val containers = state
            override suspend fun openRunningProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process =
                openProcess(id, arguments, cwd, environment)
            override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
                check(arguments.take(2) == listOf("/bin/sh", "-c")) { "Unexpected non-feature process: $arguments" }
                val process = if (!debugFailure && arguments.last().startsWith("# wawlink-feature-debug-bun\n")) {
                    // Only Bun discovery is substituted; Skill installation and discovery run production shell.
                    ProcessBuilder("/bin/sh", "-c", "exit 0").start().also {
                        it.waitFor()
                    }
                } else ProcessBuilder(arguments).directory(directory).apply {
                    environment().putAll(environment)
                    environment()["HOME"] = home.absolutePath
                    environment()["PATH"] = "${brokenBun.parentFile.absolutePath}:${System.getenv("PATH")}"
                }.start()
                synchronized(children) { children += process }
                return process
            }
        }
        val app = AppCoordinator(runtime, OmpProcessPool(runtime, directory.resolve("pool")), directory.resolve("app"))
        override fun close() {
            app.close()
            synchronized(children) { children.forEach { it.destroyForcibly(); it.waitFor() } }
            directory.deleteRecursively()
        }
    }

    @Test fun missingBunAtStartupAndFeatureDirectoryKeepsHostNoticeAndRepairEmpty() = runBlocking {
        FeatureFixture().use { fixture ->
            withTimeout(5_000) { fixture.app.features.errors.first { "guest" in it } }
            val catalog = fixture.app.features.catalog("guest")
            assertTrue(catalog.errors.isEmpty())
            assertTrue(fixture.directory.resolve("home/.omp/agent/skills/wawlink-functions/SKILL.md").isFile)
            assertTrue(fixture.app.features.errors.value["guest"].orEmpty().isEmpty())
            assertNull(fixture.app.notice.value)
            assertNull(fixture.app.ompSetup.value)
        }
    }

    @Test fun realDebuggerInstallationFailureProducesDiagnosticHostNoticeWithoutOmpRepair() = runBlocking {
        FeatureFixture(debugFailure = true).use { fixture ->
            val notice = withTimeout(5_000) { fixture.app.notice.first { it?.error == true } }!!
            assertTrue(notice.text.contains("功能调试器安装失败"))
            assertTrue(notice.text.contains("fchown: EPERM"))
            assertTrue(notice.details.orEmpty().contains("fchown: EPERM"))
            val catalog = fixture.app.features.catalog("guest")
            assertTrue(catalog.errors.any { it.contains("功能调试器安装失败") && it.contains("fchown: EPERM") })
            assertTrue(fixture.directory.resolve("home/.omp/agent/skills/wawlink-functions/SKILL.md").isFile)
            assertNull(fixture.app.ompSetup.value)
        }
    }

    private class Fixture(private val waitForInput: Boolean = false, private val installerExit: Int = 0) : AutoCloseable {
        private val directory = Files.createTempDirectory("wawlink-interruption-").toFile()
        private val base = LocalContainerRuntime(RuntimePaths(directory.resolve("runtime")))
        val containers = MutableStateFlow(listOf(ContainerInfo(ContainerConfig("guest", "测试容器", directory.resolve("root").path))))
        val probes = AtomicInteger()
        val opened = Channel<Process>(Channel.UNLIMITED)
        val packageOpened = Channel<Process>(Channel.UNLIMITED)
        val attachPackage = CompletableDeferred<Unit>()
        private val children = mutableListOf<Process>()
        private val runtime = object : ContainerRuntime by base {
            override val containers = this@Fixture.containers
            override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
                check(arguments.last() == "--inspect")
                probes.incrementAndGet()
                val script = (if (waitForInput) "read release; " else "") + "printf 'WAWLINK_OMP_CHECK\\tMISSING\\tOMP is missing\\n'"
                return ProcessBuilder("/bin/sh", "-c", script).start().also { synchronized(children) { children += it }; opened.send(it) }
            }
            override suspend fun openRunningProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
                if (arguments.lastOrNull() != "--inspect") return base.openRunningProcess(id, arguments, cwd, environment)
                probes.incrementAndGet()
                return spawn("printf 'WAWLINK_OMP_CHECK\\tREADY\\thealthy\\n'")
            }
            override suspend fun openPackageTerminal(id: String, stopGeneration: Long, command: String, beforeStart: suspend () -> Unit): TerminalSession {
                beforeStart()
                containers.value = containers.value.map { it.copy(running = true, runGeneration = 1) }
                val child = spawn("read gate; exit $installerExit")
                packageOpened.send(child)
                attachPackage.await()
                return object : TerminalSession {
                    override val process = child
                    override val processId = child.pid()
                    override val isAlive = MutableStateFlow(true)
                    override val runGeneration = 1L
                    override suspend fun awaitExit() = runInterruptible(Dispatchers.IO) { process.waitFor() }.also { isAlive.value = false }
                    override fun resize(columns: Int, rows: Int, cellWidthPixels: Int, cellHeightPixels: Int) = Unit
                    override fun close() { if (process.isAlive) process.destroyForcibly() }
                }
            }
            override suspend fun closeProcess(id: String, process: Process) {
                withContext(NonCancellable + Dispatchers.IO) {
                    if (process.isAlive) process.destroyForcibly()
                    process.waitFor()
                }
            }
        }
        private fun spawn(script: String): Process = ProcessBuilder("/bin/sh", "-c", script).start().also {
            synchronized(children) { children += it }
        }
        val app = AppCoordinator(runtime, OmpProcessPool(runtime, directory.resolve("pool")), directory.resolve("app"))
        fun interrupt() { containers.value = containers.value.map { it.copy(stopGeneration = it.stopGeneration + 1) } }
        override fun close() {
            attachPackage.complete(Unit)
            app.close()
            synchronized(children) { children.forEach { it.destroyForcibly(); it.waitFor() } }
            directory.deleteRecursively()
        }
    }

    @Test fun automaticRepairRequestExpiresAcrossStopAndFreshCheckStillOffersRepair() = runBlocking {
        Fixture().use { fixture ->
            fixture.app.checkOmpInstallation("guest")
            val first = withTimeout(5_000) { fixture.app.ompSetup.first { it != null } }!!
            assertEquals(0L, first.stopGeneration)
            fixture.interrupt()
            withTimeout(5_000) { fixture.app.ompSetup.first { it == null } }
            assertNull(fixture.app.confirmOmpSetup(first), "A stale dialog must not authorize installation")
            fixture.app.checkOmpInstallation("guest")
            val next = withTimeout(5_000) { fixture.app.ompSetup.first { it != null } }!!
            assertEquals(1L, next.stopGeneration)
            assertNotEquals(first.id, next.id)
            assertEquals(2, fixture.probes.get())
        }
    }

    @Test fun delayedInstallationCheckCannotOfferRepairAfterContainerStop() = runBlocking {
        Fixture(waitForInput = true).use { fixture ->
            val check = async { fixture.app.checkOmpInstallation("guest") }
            val process = withTimeout(5_000) { fixture.opened.receive() }
            fixture.interrupt()
            runCatching { process.outputStream.write("continue\n".toByteArray()); process.outputStream.flush() }
            withTimeout(5_000) { check.await() }
            assertNull(fixture.app.ompSetup.value)
            assertNull(fixture.app.notice.value)
            assertEquals(1, fixture.probes.get())
        }
    }

    @Test fun approvalFromPreviousContainerLifetimeCannotOpenInstallationTerminal() = runBlocking {
        Fixture().use { fixture ->
            fixture.app.requestOmpInstallation("guest")
            val request = withTimeout(5_000) { fixture.app.ompSetup.first { it != null } }!!
            val approval = fixture.app.confirmOmpSetup(request)
            assertTrue(approval != null)
            fixture.interrupt()
            assertFailsWith<CancellationException> { fixture.app.openTerminal("guest", approval) }
            assertEquals(0, fixture.probes.get())
            assertNull(fixture.app.notice.value)
        }
    }

    @Test fun closingAViewDoesNotCancelAuthorizedInstallerAndOnlySuccessRunsPostCheck() = runBlocking {
        for (exit in listOf(23, 0)) Fixture(installerExit = exit).use { fixture ->
            withTimeout(5_000) {
                val app = fixture.app
                app.requestOmpInstallation("guest")
                val approval = requireNotNull(app.confirmOmpSetup(app.ompSetup.first { it != null }!!))
                val outcome = requireNotNull(app.ompInstallationOutcome("guest", approval))
                val detachedView = async { app.openTerminal("guest", approval) }
                val process = fixture.packageOpened.receive()
                detachedView.cancelAndJoin()
                assertTrue(process.isAlive)
                assertNull(outcome.value)
                fixture.attachPackage.complete(Unit)
                val reattached = app.openTerminal("guest", approval)
                assertSame(process, reattached.process)
                reattached.process.outputStream.apply { write("finish\n".toByteArray()); flush() }
                assertEquals(exit != 0, outcome.first { it != null }!!.failed)
                assertEquals(if (exit == 0) 1 else 0, fixture.probes.get())
                app.finishOmpInstallation("guest", approval)
                assertNull(app.ompInstallationOutcome("guest", approval))
            }
        }
    }
}
