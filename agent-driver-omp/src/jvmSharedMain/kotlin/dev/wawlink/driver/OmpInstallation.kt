package dev.wawlink.driver

import dev.wawlink.runtime.ContainerRuntime
import dev.wawlink.runtime.GuestPackageScripts
import dev.wawlink.runtime.wasInterrupted
import dev.wawlink.runtime.withContainerFileOperation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

enum class OmpInstallationState { READY, MISSING, BROKEN }
data class OmpInstallationCheck(val state: OmpInstallationState, val message: String, val diagnostic: String = "")
internal const val OMP_REPAIR_NEEDED = "容器中的 OMP 缺失或安装损坏，请在终端中安装修复"

/** Installation health only: the guest probe uses an empty temporary HOME, not user settings or credentials. */
object OmpInstallation {
    private val script by lazy {
        checkNotNull(javaClass.getResourceAsStream("/omp-install.sh")) { "缺少打包的 OMP 安装修复脚本" }
            .bufferedReader(Charsets.UTF_8).use { it.readText() }
            .replace("# WAWLINK_GUEST_PACKAGES", GuestPackageScripts.preamble)
    }

    fun command(): String = script

    /** Startup failures only nominate a check; a failed check is not evidence of a broken install. */
    internal suspend fun needsRepair(runtime: ContainerRuntime, containerId: String, stopGeneration: Long, runGeneration: Long): Boolean = try {
        withContainerFileOperation(runtime, containerId, stopGeneration, runningOnly = true, expectedRunGeneration = runGeneration) {
            inspect(runtime, containerId, stopGeneration, runningOnly = true)
        }.state != OmpInstallationState.READY
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        false
    }

    suspend fun inspect(
        runtime: ContainerRuntime,
        containerId: String,
        stopGeneration: Long = runtime.containers.value.firstOrNull { it.config.id == containerId }?.stopGeneration
            ?: throw OmpProcessStoppedException(),
        runningOnly: Boolean = false,
    ): OmpInstallationCheck = coroutineScope {
        fun interrupted() = runtime.wasInterrupted(containerId, stopGeneration) ||
            (runningOnly && runtime.containers.value.none { it.config.id == containerId && it.running })
        if (interrupted()) throw OmpProcessStoppedException()
        // Do not use execute(): the configured login shell may itself be missing or broken.
        // Engine failures and missing probe frames must propagate rather than suggesting reinstall.
        var owned: Process? = null
        var inspectionFailure: Throwable? = null
        try {
            val owner = currentCoroutineContext()
            val process = withContext(NonCancellable) {
                owner.ensureActive()
                if (interrupted()) throw OmpProcessStoppedException()
                val arguments = listOf("/bin/sh", "-c", script, "wawlink-omp-install", "--inspect")
                withContainerFileOperation(runtime, containerId, stopGeneration, runningOnly) {
                    (if (runningOnly) runtime.openRunningProcess(containerId, arguments, "/")
                    else runtime.openProcess(containerId, arguments, "/")).also { owned = it }
                }
            }
            // Readers live outside the timeout so cleanup can close the guest before joining them.
            val stdout = async(Dispatchers.IO) { runCatching { process.inputStream.bufferedReader().use { it.readText() } } }
            val stderr = async(Dispatchers.IO) { runCatching { process.errorStream.bufferedReader().use { it.readText() } } }
            val check = withTimeoutOrNull(90_000) {
                val checkJob = currentCoroutineContext().job
                val watch = launch(start = CoroutineStart.UNDISPATCHED) {
                    runtime.containers.first { interrupted() }
                    checkJob.cancel()
                }
                try {
                    val exit = runInterruptible(Dispatchers.IO) { process.waitFor() }
                    val output = stdout.await()
                    val diagnostic = stderr.await()
                    if (interrupted()) throw OmpProcessStoppedException()
                    // Interpret evidence as a value until cleanup can attach to the original failure.
                    runCatching {
                        val details = diagnostic.getOrThrow().trim()
                        if (exit != 0) throw IllegalStateException(
                            "OMP 安装检查无法完成（$exit）；请查看详情",
                            IllegalStateException(details.ifEmpty { "客体自检退出，未提供诊断" }),
                        )
                        val result = decodeCheck(output.getOrThrow())
                        if (result.state == OmpInstallationState.BROKEN && details.isNotEmpty()) result.copy(diagnostic = details) else result
                    }
                } finally { watch.cancel() }
            } ?: error("OMP 安装检查超时（90 秒）")
            check.getOrThrow()
        } catch (failure: Throwable) {
            val reported = if (currentCoroutineContext().isActive && interrupted() && failure !is OmpProcessStoppedException) OmpProcessStoppedException() else failure
            inspectionFailure = reported
            throw reported
        } finally {
            try {
                owned?.let { runtime.closeProcess(containerId, it) }
            } catch (cleanupFailure: Exception) {
                val primary = inspectionFailure
                if (primary == null) throw cleanupFailure
                if (primary !== cleanupFailure) primary.addSuppressed(cleanupFailure)
            }
        }
    }

    internal fun decodeCheck(output: String): OmpInstallationCheck {
        val frames = output.lineSequence().filter { it.startsWith("WAWLINK_OMP_CHECK\t") }.toList()
        check(frames.size == 1) { "容器未返回唯一有效的 OMP 安装检查结果；请检查容器引擎" }
        val parts = frames.single().split('\t', limit = 3)
        val state = OmpInstallationState.entries.firstOrNull { it.name == parts.getOrNull(1) }
        check(state != null && parts.size == 3 && parts[2].isNotBlank()) { "OMP 安装检查协议无效" }
        return OmpInstallationCheck(state, parts[2])
    }
}
