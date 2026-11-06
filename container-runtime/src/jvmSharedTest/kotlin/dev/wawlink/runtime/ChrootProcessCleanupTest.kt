package dev.wawlink.runtime

import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Real POSIX shell over a synthetic /proc. No real signal or Android privilege is simulated. */
class ChrootProcessCleanupTest {
    private class Fixture : AutoCloseable {
        val directory = Files.createTempDirectory("wawlink-process-cleanup-").toFile()
        val root = File(directory, "guest ' space \\ root").apply { mkdir() }
        val proc = File(directory, "proc").apply { mkdir() }
        val signals = File(directory, "signals")
        val sleeps = File(directory, "sleeps")
        init {
            put(90000, File("/"))
            Files.createSymbolicLink(File(proc, "self").toPath(), File("90000").toPath())
        }
        fun stat(pid: Int, start: Int = 100, state: String = "S", flags: Int = 0): String =
            "$pid (opaque ) command) " + (mutableListOf(state, "1", "$pid", "$pid", "0", "0", "$flags") +
                List(12) { "0" } + start.toString() + List(5) { "0" }).joinToString(" ")
        fun put(pid: Int, target: File = root, start: Int = 100, state: String = "S", flags: Int = 0) {
            val entry = File(proc, "$pid").apply { mkdir() }
            File(entry, "stat").writeText(stat(pid, start, state, flags))
            Files.createSymbolicLink(File(entry, "root").toPath(), target.toPath())
        }
        fun run(hooks: String = "", signalBody: String = "rm -rf \"\$proc/\$2\"", sleepBody: String = ":"): Process {
            val prefix = """
                signals=${shellQuote(signals.path)}
                sleeps=${shellQuote(sleeps.path)}
                kill() { printf '%s %s\n' "${'$'}1" "${'$'}2" >> "${'$'}signals"; $signalBody; }
                sleep() { printf 'wait\n' >> "${'$'}sleeps"; $sleepBody; }
                $hooks
            """.trimIndent()
            return ProcessBuilder("/bin/sh", "-c", prefix + "\n" + ChrootProcessCleanup.script(root, proc, 2, 2))
                .redirectErrorStream(true).start()
        }
        fun output(process: Process): Pair<Int, String> {
            val text = process.inputStream.bufferedReader().use { it.readText() }
            return process.waitFor() to text
        }
        fun signaled(): List<String> = if (signals.exists()) signals.readLines() else emptyList()
        override fun close() { RootfsArchives.deleteTree(directory) }
    }

    @Test fun reclaimsReparentedGuestsAndNestedRootsButNotHostsSharingDeviceDescriptors() {
        Fixture().use { f ->
            f.put(11) // All synthetic PPIDs are 1: none are in the tracked tree.
            f.put(12, File(f.root, "nested root").apply { mkdir() })
            f.put(13, File("/"))
            f.put(14, File(f.root.path + "-neighbor"))
            File(f.proc, "13/fd").mkdir()
            Files.createSymbolicLink(File(f.proc, "13/fd/0").toPath(), File("/dev/null").toPath())
            val result = f.output(f.run())
            assertEquals(0, result.first, result.second)
            assertEquals(listOf("-TERM 11", "-TERM 12"), f.signaled())
            assertTrue(File(f.proc, "13/stat").exists())
            assertTrue(File(f.proc, "14/stat").exists())
        }
    }

    @Test fun rechecksPidAndRootBeforeSignaling() {
        for (changeRoot in listOf(false, true)) Fixture().use { f ->
            f.put(11)
            val replacement = if (changeRoot) "rm ${shellQuote(File(f.proc, "11/root").path)}; ln -s / ${shellQuote(File(f.proc, "11/root").path)}"
                else "printf '%s' ${shellQuote(f.stat(11, 101))} > ${shellQuote(File(f.proc, "11/stat").path)}"
            val hooks = """
                readlink() {
                    if [ "${'$'}1" = ${shellQuote(File(f.proc, "11/root").path)} ]; then
                        counter=${shellQuote(File(f.directory, "reads").path)}
                        if [ -e "${'$'}counter" ]; then $replacement; else : > "${'$'}counter"; fi
                    fi
                    command readlink "${'$'}@"
                }
            """.trimIndent()
            val result = f.output(f.run(hooks))
            assertTrue(result.first != 0, result.second)
            assertTrue(f.signaled().isEmpty(), "Changed identities must not authorize even TERM")
        }
    }

    @Test fun killWaitsForActualExitAndReportsPersistentSurvivors() {
        for (exits in listOf(false, true)) Fixture().use { f ->
            f.put(11)
            val signal = "if [ \"\$1\" = -KILL ]; then : > ${shellQuote(File(f.directory, "killed").path)}; fi"
            val sleep = if (exits) "if [ -e ${shellQuote(File(f.directory, "killed").path)} ]; then rm -rf \"\$proc/11\"; fi" else ":"
            val result = f.output(f.run(signalBody = signal, sleepBody = sleep))
            assertEquals(exits, result.first == 0, result.second)
            assertEquals(listOf("-TERM 11", "-KILL 11"), f.signaled())
            assertTrue(f.sleeps.readLines().size >= 3, "There must be a bounded wait after KILL, not just after TERM")
        }
    }

    @Test fun rescansOnlyAtPhaseBoundariesToFindForksDuringTerm() {
        Fixture().use { f ->
            f.put(11)
            val stat = shellQuote(f.stat(12))
            val signal = """
                if [ "${'$'}2" = 11 ]; then
                    mkdir "${'$'}proc/12"
                    printf '%s' $stat > "${'$'}proc/12/stat"
                    ln -s "${'$'}root" "${'$'}proc/12/root"
                fi
                rm -rf "${'$'}proc/${'$'}2"
            """.trimIndent()
            val result = f.output(f.run(signalBody = signal))
            assertEquals(0, result.first, result.second)
            assertEquals(listOf("-TERM 11", "-TERM 12"), f.signaled())
        }
    }

    @Test fun ignoresZombiesKernelThreadsNaturalExitsAndUnreadableUnknownHosts() {
        Fixture().use { f ->
            f.put(11, state = "Z")
            f.put(12, flags = 2097152)
            File(f.proc, "12/root").delete()
            f.put(13)
            val hooks = "readlink() { if [ \"\$1\" = ${shellQuote(File(f.proc, "13/root").path)} ]; then rm -rf ${shellQuote(File(f.proc, "13").path)}; return 1; fi; command readlink \"\$@\"; }"
            val result = f.output(f.run(hooks))
            assertEquals(0, result.first, result.second)
            assertTrue(f.signaled().isEmpty())
            f.put(14, File("/"))
            File(f.proc, "14/root").delete()
            File(f.proc, "14/stat").delete()
            f.put(15)
            val unknown = f.output(f.run())
            assertEquals(0, unknown.first, unknown.second)
            assertEquals(listOf("-TERM 15"), f.signaled())
            assertTrue(File(f.proc, "14").isDirectory, "Unreadable unknown hosts must remain untouched")
        }
    }

    @Test fun pinnedCandidatesStillFailClosedOnRootOrStatReadDenial() {
        for (denyRoot in listOf(false, true)) Fixture().use { f ->
            f.put(11)
            val tool = if (denyRoot) "readlink" else "cat"
            val target = File(f.proc, "11/" + if (denyRoot) "root" else "stat")
            val hooks = """
                $tool() {
                    if [ "${'$'}1" = ${shellQuote(target.path)} ]; then
                        counter=${shellQuote(File(f.directory, "reads").path)}
                        if [ -e "${'$'}counter" ]; then return 1; else : > "${'$'}counter"; fi
                    fi
                    command $tool "${'$'}@"
                }
            """.trimIndent()
            val result = f.output(f.run(hooks))
            assertTrue(result.first != 0, result.second)
            assertTrue(result.second.contains(if (denyRoot) "root" else "stat"))
            assertTrue(f.signaled().isEmpty(), "A matched root does not waive subsequent identity/root checks")
            assertFalse(File(f.directory, "killed").exists())
        }
    }

    @Test fun inaccessibleProcFailsWithoutSendingSignals() {
        Fixture().use { f ->
            File(f.proc, "self").delete()
            val result = f.output(f.run())
            assertTrue(result.first != 0, result.second)
            assertTrue(result.second.contains("Root /proc"))
            assertTrue(f.signaled().isEmpty())
        }
    }
}
