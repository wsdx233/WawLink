package dev.wawlink.app

import android.app.Application
import android.os.Looper
import com.termux.terminal.TerminalSession
import com.termux.terminal.TerminalSessionClient
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.FilterInputStream
import java.lang.reflect.Proxy
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.Shadows.shadowOf
import org.robolectric.annotation.Config
import org.robolectric.annotation.LooperMode

@RunWith(RobolectricTestRunner::class)
@Config(sdk = [35], application = Application::class)
@LooperMode(LooperMode.Mode.PAUSED)
class TerminalOutputTest {
    @Test fun exitedInstallerKeepsDiagnosticBytesNotYetReadFromPty() {
        val releaseOutput = CountDownLatch(1)
        val waiter = CompletableFuture<Thread>()
        val reader = CompletableFuture<Thread>()
        val finalLine = "original package failure"
        val input = object : FilterInputStream(ByteArrayInputStream("$finalLine\r\n".toByteArray())) {
            override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
                reader.complete(Thread.currentThread())
                check(releaseOutput.await(5, TimeUnit.SECONDS))
                return super.read(buffer, offset, length)
            }
        }
        val process = object : Process() {
            override fun getInputStream() = input
            override fun getOutputStream() = ByteArrayOutputStream()
            override fun getErrorStream() = ByteArrayInputStream(byteArrayOf())
            override fun waitFor(): Int { waiter.complete(Thread.currentThread()); return exitValue() }
            override fun exitValue() = 23
            override fun destroy() = Unit
        }
        var finished = 0
        val client = Proxy.newProxyInstance(TerminalSessionClient::class.java.classLoader, arrayOf(TerminalSessionClient::class.java)) { _, method, _ ->
            if (method.name == "onSessionFinished") finished++
            if (method.returnType == Int::class.javaPrimitiveType) 0 else null
        } as TerminalSessionClient
        val terminal = TerminalSession(process, 42, { _, _, _, _ -> }, 100, client)
        val main = shadowOf(Looper.getMainLooper())
        terminal.updateSize(80, 24, 8, 16)
        fun awaitThread(thread: CompletableFuture<Thread>) {
            val actual = thread.get(5, TimeUnit.SECONDS)
            actual.join(5_000)
            assertFalse(actual.isAlive)
        }
        try {
            awaitThread(waiter)
            main.idle()
            assertEquals(0, finished)
        } finally {
            releaseOutput.countDown()
            awaitThread(reader)
            main.runToEndOfTasks()
        }
        assertEquals(1, finished)
        assertEquals(23, terminal.exitStatus)
        assertTrue(terminal.emulator.screen.transcriptText.contains(finalLine))
    }
}
