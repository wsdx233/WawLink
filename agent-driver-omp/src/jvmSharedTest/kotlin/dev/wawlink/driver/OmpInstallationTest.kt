package dev.wawlink.driver

import dev.wawlink.runtime.ContainerConfig
import dev.wawlink.runtime.ContainerInfo
import dev.wawlink.runtime.ContainerRuntime
import dev.wawlink.runtime.LocalContainerRuntime
import dev.wawlink.runtime.RuntimePaths
import java.io.File
import java.nio.file.attribute.PosixFilePermissions
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.runBlocking

class OmpInstallationTest {
    private class ResultProcess(output: String, private val status: Int = 0, diagnostic: String = "") : Process() {
        private val stdout = ByteArrayInputStream(output.toByteArray())
        private val stderr = ByteArrayInputStream(diagnostic.toByteArray())
        private val stdin = ByteArrayOutputStream()
        override fun getInputStream() = stdout
        override fun getErrorStream() = stderr
        override fun getOutputStream() = stdin
        override fun waitFor() = status
        override fun exitValue() = status
        override fun destroy() = Unit
        override fun isAlive() = false
    }

    private suspend fun fixture(block: suspend (ContainerRuntime) -> Unit) {
        val directory = Files.createTempDirectory("wawlink-install-check-").toFile()
        val base = LocalContainerRuntime(RuntimePaths(directory))
        val containerState = MutableStateFlow(listOf(ContainerInfo(ContainerConfig("guest", "验证", directory.path), running = true)))
        val runtime = object : ContainerRuntime by base { override val containers = containerState }
        try { block(runtime) } finally { base.close(); directory.deleteRecursively() }
    }

    @Test fun engineLaunchFailureIsNotMisreportedAsMissingOmp() = runBlocking {
        fixture { base ->
            val failure = IllegalStateException("Cannot launch container engine: missing executable")
            val runtime = object : ContainerRuntime by base {
                override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process = throw failure
            }
            assertFailsWith<IllegalStateException> { OmpInstallation.inspect(runtime, "guest") }
        }
    }

    @Test fun failedEngineExitCannotForgeAMissingInstallationResult() = runBlocking {
        fixture { base ->
            val runtime = object : ContainerRuntime by base {
                override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process =
                    ResultProcess("WAWLINK_OMP_CHECK\tMISSING\tmissing\n", 127, "guest interpreter failed")
            }
            val error = assertFailsWith<IllegalStateException> { OmpInstallation.inspect(runtime, "guest") }
            assertTrue(error.message.orEmpty().contains("127"))
            assertTrue(generateSequence(error as Throwable) { it.cause }.any { it.message == "guest interpreter failed" }, error.stackTraceToString())
        }
    }

    @Test fun incompleteAmbiguousAndUnknownProbeFramesAreErrorsNotReinstallSuggestions() {
        for (output in listOf("", "bun not found", "WAWLINK_OMP_CHECK\tUNKNOWN\tmessage\n",
            "WAWLINK_OMP_CHECK\tREADY\n", "WAWLINK_OMP_CHECK\tREADY\t \n",
            "WAWLINK_OMP_CHECK\tREADY\tok\nWAWLINK_OMP_CHECK\tMISSING\tmissing\n")) {
            assertFailsWith<IllegalStateException>(output) { OmpInstallation.decodeCheck(output) }
        }
    }

    @Test fun packageLockPrecedesPrerequisitesAndFailedRepairReleasesIt() {
        val directory = Files.createTempDirectory("wawlink-install-lock-").toFile()
        try {
            val tools = File(directory, "tools").apply { mkdirs() }
            val lock = File(directory, "opt/.wawlink-omp-install.lock").apply { mkdirs() }
            File(directory, "os-release").writeText("ID=ubuntu\nVERSION_ID=24.04\n")
            val log = File(directory, "calls")
            fun executable(name: String, body: String) = File(tools, name).apply {
                writeText("#!/bin/sh\n$body\n")
                assertTrue(setExecutable(true))
            }
            executable("id", "echo 0")
            executable("dpkg", "[ -d '${lock.path}' ] || exit 92; umask > '${log.path}'; exit 43")
            executable("apt-get", "printf 'unexpected apt\\n' >> '${log.path}'")
            val script = OmpInstallation.command()
                .replace("/etc/os-release", File(directory, "os-release").path)
                .replace("/opt", File(directory, "opt").path)
                .replace("/usr", File(directory, "usr").path)
                .replace(Regex("(?m)^export PATH=.*$"), "export PATH=${tools.path}:/usr/bin:/bin")
            fun runInstaller(): Int {
                val process = ProcessBuilder("/bin/sh", "-c", script).redirectErrorStream(true).start()
                process.inputStream.use { it.readBytes() }
                return process.waitFor()
            }
            assertEquals(1, runInstaller())
            assertTrue(!log.exists())
            assertTrue(lock.delete())
            assertEquals(43, runInstaller())
            assertEquals("0022", log.readText().trim())
            assertTrue(!lock.exists())
        } finally { directory.deleteRecursively() }
    }

    @Test fun inspectionEnvironmentFailuresNeverSuggestReinstallAndKeepDiagnostics() = runBlocking {
        fixture { base ->
            for (diagnostic in listOf("EACCES: private native addon", "OMP RPC probe timed out after 45 seconds")) {
                val runtime = object : ContainerRuntime by base {
                    override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process =
                        ResultProcess("", 75, diagnostic)
                }
                val failure = assertFailsWith<IllegalStateException> { OmpInstallation.inspect(runtime, "guest") }
                assertTrue(generateSequence(failure as Throwable) { it.cause }.any { it.message == diagnostic }, failure.stackTraceToString())
                assertTrue(!failure.message.orEmpty().contains(diagnostic))
            }
            val runtime = object : ContainerRuntime by base {
                override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process =
                    ResultProcess("WAWLINK_OMP_CHECK\tBROKEN\tnative failure\n", diagnostic = "ELF symbol missing: actual native diagnostic")
            }
            val result = OmpInstallation.inspect(runtime, "guest")
            assertEquals(OmpInstallationState.BROKEN, result.state)
            assertTrue(result.diagnostic.contains("ELF symbol missing"))
            assertTrue(!result.message.contains("ELF symbol missing"))
        }
    }

    @Test fun inspectionClosesOwnedProcessWithoutMaskingOriginalFailure() = runBlocking {
        fixture { base ->
            var closed = 0
            val child = ResultProcess("", 75, "original isolated probe failure")
            val runtime = object : ContainerRuntime by base {
                override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process = child
                override suspend fun closeProcess(id: String, process: Process) {
                    assertTrue(process === child)
                    closed++
                    error("cleanup failure")
                }
            }
            val failure = assertFailsWith<IllegalStateException> { OmpInstallation.inspect(runtime, "guest") }
            assertEquals(1, closed)
            val chain = generateSequence(failure as Throwable) { it.cause }.toList()
            assertTrue(chain.any { it.message == "original isolated probe failure" }, failure.stackTraceToString())
            assertEquals("cleanup failure", chain.flatMap { it.suppressed.asList() }.single().message)
        }
    }

    @Test fun realPosixInspectionDistinguishesProbeTimeoutFromBrokenNative() {
        val directory = Files.createTempDirectory("wawlink-inspect-shell-").toFile()
        try {
            File(directory, "opt/omp/dist").mkdirs()
            File(directory, "opt/omp/dist/cli.js").writeText("fixture")
            val bin = File(directory, "usr/local/bin").apply { mkdirs() }
            File(bin, "omp").apply { writeText("#!/bin/sh\nexit 0\n"); setExecutable(true) }
            val script = OmpInstallation.command()
                .replace("/opt", File(directory, "opt").path)
                .replace("/usr", File(directory, "usr").path)
            for ((status, expectedExit) in listOf(75 to 75, 126 to 75, 1 to 0)) {
                File(bin, "bun").apply {
                    writeText("#!/bin/sh\nprintf 'probe diagnostic $status\\n' >&2\nexit $status\n")
                    setExecutable(true)
                }
                val process = ProcessBuilder("/bin/sh", "-c", script, "fixture", "--inspect").start()
                val stdout = process.inputStream.bufferedReader().use { it.readText() }
                val stderr = process.errorStream.bufferedReader().use { it.readText() }
                assertEquals(expectedExit, process.waitFor())
                assertTrue(stderr.contains("probe diagnostic $status"))
                if (expectedExit == 0) assertTrue(stdout.contains("WAWLINK_OMP_CHECK\tBROKEN\t"))
                else assertTrue(!stdout.contains("WAWLINK_OMP_CHECK"))
            }
        } finally { directory.deleteRecursively() }
    }

    @Test fun restrictiveUmaskPublishesOnlyReadableExecutableProgramTree() {
        val directory = Files.createTempDirectory("wawlink-install-modes-").toFile()
        try {
            val installer = OmpInstallation.command()
            val publication = installer.substringAfter("# executable payload becomes public; staging, probe HOME and backups stay 700.\n")
                .substringBefore("progress 'Switching validated installation")
            assertTrue(publication.contains("find \"${'$'}stage/omp\""))
            val process = ProcessBuilder("/bin/sh", "-c", """
                set -eu
                umask 0077
                stage='${directory.path}/stage'
                mkdir -p "${'$'}stage/omp/node_modules" "${'$'}stage/home"
                printf 'native bytes' > "${'$'}stage/omp/node_modules/native.node"
                printf '#!/bin/sh\n' > "${'$'}stage/omp/program"
                chmod 700 "${'$'}stage/omp/program"
                printf 'private configuration' > "${'$'}stage/home/config.yml"
                $publication
                mv "${'$'}stage/omp" '${directory.path}/published'
            """.trimIndent()).start()
            val diagnostic = process.errorStream.bufferedReader().use { it.readText() }
            assertEquals(0, process.waitFor(), diagnostic)
            fun mode(path: String) = PosixFilePermissions.toString(Files.getPosixFilePermissions(File(directory, path).toPath()))
            assertEquals("rwxr-xr-x", mode("published"))
            assertEquals("rwxr-xr-x", mode("published/node_modules"))
            assertEquals("rw-r--r--", mode("published/node_modules/native.node"))
            assertEquals("rwxr-xr-x", mode("published/program"))
            assertEquals("rwx------", mode("stage"))
            assertEquals("rwx------", mode("stage/home"))
            assertEquals("rw-------", mode("stage/home/config.yml"))
        } finally { directory.deleteRecursively() }
    }

    @Test fun staleInspectionAndRunningOnlyInspectionNeverOpenStoppedContainer() = runBlocking {
        fixture { base ->
            val containerState = MutableStateFlow(base.containers.value.map { it.copy(running = false, stopGeneration = 1) })
            var starts = 0
            val runtime = object : ContainerRuntime by base {
                override val containers = containerState
                override suspend fun openProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
                    starts++
                    return ResultProcess("WAWLINK_OMP_CHECK\tBROKEN\tunused\n")
                }
            }
            assertFailsWith<OmpProcessStoppedException> { OmpInstallation.inspect(runtime, "guest", stopGeneration = 0) }
            assertFailsWith<OmpProcessStoppedException> { OmpInstallation.inspect(runtime, "guest", runningOnly = true) }
            assertEquals(0, starts)
        }
    }

    @Test fun inspectionResultFromBeforeRapidRestartIsInterruptedInsteadOfSuggestingRepair() = runBlocking {
        fixture { base ->
            val containerState = MutableStateFlow(base.containers.value)
            val opened = CompletableDeferred<Unit>()
            val gate = CompletableDeferred<Process>()
            var starts = 0
            val runtime = object : ContainerRuntime by base {
                override val containers = containerState
                override suspend fun openRunningProcess(id: String, arguments: List<String>, cwd: String, environment: Map<String, String>): Process {
                    starts++
                    opened.complete(Unit)
                    return gate.await()
                }
            }
            val inspection = async { runCatching { OmpInstallation.inspect(runtime, "guest", runningOnly = true) } }
            withTimeout(5_000) { opened.await() }
            containerState.value = containerState.value.map { it.copy(running = false, stopGeneration = 1) }
            containerState.value = containerState.value.map { it.copy(running = true) }
            gate.complete(ResultProcess("WAWLINK_OMP_CHECK\tBROKEN\tstale result\n"))
            assertTrue(withTimeout(5_000) { inspection.await() }.exceptionOrNull() is OmpProcessStoppedException)
            assertEquals(1, starts)
        }
    }

}
