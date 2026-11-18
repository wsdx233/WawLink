package dev.wawlink.driver

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal data class OmpRpcTiming(
    val pollMs: Long = 1_000,
    val idleMs: Long = 5_000,
    val probeMs: Long = 25_000,
    val requestMs: Long = 45_000,
    val writeMs: Long = 25_000,
)

internal class OmpRpcTimeoutException(operation: String, timeoutMs: Long) :
    IllegalStateException("OMP 请求超时：$operation（${timeoutMs}ms）；结果尚未确认，请核对历史，不要重复发送")

internal class OmpProtocolException(message: String) : IOException(message)
internal class OmpPipeException(message: String, val writeTimedOut: Boolean = false) : IOException(message)

private val ioAuthorization = Regex("(?i)(\\bAuthorization\\s*[:=]\\s*(?:(?:Bearer|Basic)\\s+)?|\\b(?:Bearer|Basic)\\s+)[^\\s,;]+")
private val ioCredential = Regex("(?i)([\\\"']?(?:token|api[_-]?key|password|passwd|secret)[\\\"']?\\s*[:=]\\s*)(?:\\\"[^\\\"]*\\\"|'[^']*'|[^\\s,;]+)")
private val ioUrlUser = Regex("(?i)([a-z][a-z0-9+.-]*://)([^/\\s:@]+(?::[^/@\\s]*)?@)")
private val ioUrlQuery = Regex("(?i)([a-z][a-z0-9+.-]*://[^\\s\"'<>?#]+)[?#][^\\s\"'<>]*")
private val ioToken = Regex("\\b(?:sk-[A-Za-z0-9_-]+|eyJ[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)?)\\b")

/** IO diagnostics only: rejected protocol/parser source must never enter this formatter. */
internal fun ompIoDiagnostic(failure: Throwable): String {
    val seen = HashSet<Throwable>()
    var current: Throwable? = failure
    val details = buildString {
        while (true) {
            val cause = current ?: break
            if (!seen.add(cause)) break
            if (isNotEmpty()) append(" <- ")
            append(cause.javaClass.simpleName)
            cause.message?.let { append(": "); append(it) }
            current = cause.cause
        }
    }
    var safe = ioAuthorization.replace(details) { "${it.groupValues[1]}[redacted]" }
    safe = ioUrlUser.replace(safe) { "${it.groupValues[1]}[redacted]@" }
    safe = ioUrlQuery.replace(safe) { "${it.groupValues[1]}?[redacted]" }
    safe = ioCredential.replace(safe) { "${it.groupValues[1]}[redacted]" }
    return ioToken.replace(safe, "[redacted]")
}

/** Only accepted protocol frames, including validated partial chunks, reset silence. */
internal class OmpLiveness {
    private val frames = AtomicLong()
    @Volatile var lastFrameNanos: Long = System.nanoTime()
        private set
    val hasReceivedFrame: Boolean get() = frames.get() != 0L
    private var missed = 0

    @Synchronized fun received() {
        lastFrameNanos = System.nanoTime()
        frames.incrementAndGet()
        missed = 0
    }
    fun probeStarted(): Long = frames.get()
    @Synchronized fun timedOut(startedAt: Long): Boolean {
        if (frames.get() != startedAt) { missed = 0; return false }
        return ++missed >= 3
    }
    @Synchronized fun answered() { missed = 0 }
}

/** The waiting coroutine is bounded even when a platform pipe write ignores interruption. */
internal suspend fun writeOmpFrame(
    scope: CoroutineScope, mutex: Mutex, child: Process, bytes: ByteArray, timeoutMs: Long,
    checkCurrent: () -> Unit,
) {
    val writing = scope.async(Dispatchers.IO) {
        mutex.withLock {
            checkCurrent()
            if (!child.isAlive) throw OmpPipeException("OMP 进程已退出")
            try {
                child.outputStream.write(bytes)
                child.outputStream.write('\n'.code)
                child.outputStream.flush()
            } catch (failure: IOException) {
                throw OmpPipeException("OMP 管道写入失败：${ompIoDiagnostic(failure)}，请重新连接")
            }
        }
    }
    try {
        if (withTimeoutOrNull(timeoutMs) { writing.await(); true } != true) {
            // Kill before closing stdin: close may wait on the same monitor as the blocked write.
            child.destroyForcibly()
            throw OmpPipeException("OMP 管道写入超时（${timeoutMs}ms），进程已终止，请重新连接", writeTimedOut = true)
        }
    } finally { writing.cancel() }
}
