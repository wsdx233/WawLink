package dev.wawlink.runtime

import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class TerminalInitialCommandTest {
    @Test fun invalidCommandIsRejectedBeforeOpeningATerminal() {
        assertFailsWith<IllegalArgumentException> { terminalArguments("/bin/sh", "bad\u0000script") }
    }

    @Test fun installerRunsOnceAndReturnsItsOwnExitStatus() {
        for (exit in listOf(0, 23)) {
            val script = "printf '%s\\n' \"a ' quote\"; exit $exit"
            val process = ProcessBuilder(terminalArguments("/missing/configured/login-shell", script)).redirectErrorStream(true).start()
            try {
                assertTrue(process.waitFor(5, TimeUnit.SECONDS))
                val output = process.inputStream.bufferedReader().use { it.readLines() }
                assertEquals(exit, process.exitValue(), output.toString())
                assertEquals(listOf("a ' quote"), output)
            } finally { if (process.isAlive) process.destroyForcibly() }
        }
    }
}
