package dev.wawlink.runtime

import com.sun.net.httpserver.HttpServer
import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files
import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream

class DistroDeploymentTest {
    private val distro = DistroSpec("debian", "Debian", "12", System.getProperty("os.arch"))

    private fun image(directory: File): File = File(directory, "rootfs.tar.gz").also { archive ->
        TarArchiveOutputStream(GzipCompressorOutputStream(archive.outputStream())).use { tar ->
            listOf("etc/", "bin/", "root/").forEach { name ->
                tar.putArchiveEntry(TarArchiveEntry(name).apply { mode = 0b111101101 })
                tar.closeArchiveEntry()
            }
            listOf(
                "etc/os-release" to "ID=debian\nVERSION_ID=12\n".toByteArray(),
                "etc/passwd" to "root:x:0:0::/root:/bin/sh\n".toByteArray(),
                "etc/group" to "root:x:0:\n".toByteArray(),
                "bin/sh" to File("/bin/sh").readBytes(),
            ).forEach { (name, body) ->
                tar.putArchiveEntry(TarArchiveEntry(name).apply { size = body.size.toLong(); mode = if (name == "bin/sh") 0b111101101 else 0b110100100 })
                tar.write(body); tar.closeArchiveEntry()
            }
            tar.finish()
        }
    }

    private fun source(file: File) = DistroDownload(file.toURI().toString(),
        MessageDigest.getInstance("SHA-256").digest(file.readBytes()).joinToString("") { "%02x".format(it) }, "正在下载")

    @Test fun failedInstallRetriesOriginalParametersAndCannotRetryTwice(): Unit = runBlocking {
        val directory = Files.createTempDirectory("wawlink-distro-retry").toFile()
        val source = source(image(directory))
        val calls = AtomicInteger()
        val waiting = CountDownLatch(1)
        val release = CountDownLatch(1)
        val observed = mutableListOf<Pair<DistroSpec, MirrorSource>>()
        val runtime = LocalContainerRuntime(RuntimePaths(File(directory, "data")), { _, _ -> 0L },
            resolveDistro = { actual, mirror, control ->
                synchronized(observed) { observed += actual to mirror }
                if (calls.incrementAndGet() == 1) error("Temporary metadata failure")
                waiting.countDown()
                check(release.await(10, TimeUnit.SECONDS))
                control.checkActive()
                source
            }, runRootCommand = null)
        try {
            assertFailsWith<IllegalStateException> { runtime.installDistro(distro, "我的容器", MirrorSource.USTC, ContainerEngineType.PROOT) }
            val failed = runtime.deployments.value.single()
            assertTrue(failed.retryable)
            assertFalse(failed.cancellable)
            assertTrue(failed.error.orEmpty().contains("Temporary metadata failure"))
            val retry = async(Dispatchers.IO) { runtime.retryDistroDeployment(failed.id) }
            assertTrue(withContext(Dispatchers.IO) { waiting.await(5, TimeUnit.SECONDS) })
            val retrying = runtime.deployments.value.single()
            assertEquals(failed.id, retrying.id)
            assertTrue(retrying.cancellable)
            assertFalse(retrying.retryable)
            assertFailsWith<IllegalStateException> { runtime.retryDistroDeployment(failed.id) }
            assertFailsWith<IllegalStateException> { runtime.dismissDeployment(failed.id) }
            release.countDown()
            val installed = withTimeout(10_000) { retry.await() }
            assertEquals("我的容器", installed.name)
            assertEquals("我的容器", installed.metadata.title)
            assertEquals(ContainerEngineType.PROOT, installed.engine)
            assertEquals(listOf(distro to MirrorSource.USTC, distro to MirrorSource.USTC), observed)
            assertTrue(runtime.deployments.value.isEmpty())
            assertEquals(listOf(installed.id), runtime.containers.value.map { it.config.id })
        } finally {
            release.countDown(); runtime.close(); RootfsArchives.deleteTree(directory)
        }
    }

    @Test fun cancelClosesDownloadAndRemovesOnlyItsTaskAndPartialArchive(): Unit = runBlocking {
        val directory = Files.createTempDirectory("wawlink-distro-cancel").toFile()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val executor = Executors.newCachedThreadPool()
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        server.executor = executor
        server.createContext("/rootfs") { exchange ->
            exchange.use {
                exchange.sendResponseHeaders(200, 65536)
                exchange.responseBody.write(ByteArray(1024)); exchange.responseBody.flush()
                started.countDown()
                release.await(10, TimeUnit.SECONDS)
                runCatching { exchange.responseBody.write(ByteArray(64512)) }
            }
        }
        server.start()
        val runtime = LocalContainerRuntime(RuntimePaths(File(directory, "data")), { _, _ -> 0L },
            resolveDistro = { actual, _, _ ->
                if (actual.name == "failure") error("Keep this failed task")
                DistroDownload("http://127.0.0.1:${server.address.port}/rootfs", "a".repeat(64), "正在下载")
            }, runRootCommand = null)
        try {
            assertFailsWith<IllegalStateException> { runtime.installDistro(distro.copy(name = "failure"), "保留任务", MirrorSource.OFFICIAL, ContainerEngineType.PROOT) }
            val existing = runtime.deployments.value.single()
            val install = async(Dispatchers.IO) { runtime.installDistro(distro, "取消任务", MirrorSource.OFFICIAL, ContainerEngineType.PROOT) }
            assertTrue(withContext(Dispatchers.IO) { started.await(5, TimeUnit.SECONDS) })
            val task = withTimeout(5_000) { runtime.deployments.first { tasks -> tasks.any { it.name == "取消任务" && it.cancellable } } }.single { it.name == "取消任务" }
            val cancel = async(Dispatchers.IO) { runtime.cancelDeployment(task.id) }
            withTimeout(5_000) { runtime.deployments.first { tasks -> tasks.none { it.id == task.id } || tasks.any { it.id == task.id && it.phase == "正在取消" } } }
            // The producer is released independently so a broken disconnect cannot hang the fixture.
            release.countDown()
            withTimeout(5_000) { cancel.await(); assertFailsWith<CancellationException> { install.await() } }
            assertEquals(listOf(existing), runtime.deployments.value)
            assertTrue(runtime.containers.value.isEmpty())
            assertFalse(File(directory, "data/downloads/${task.id}.part").exists())
            assertFalse(File(directory, "data/rootfs/${task.id}").exists())
            runtime.dismissDeployment(existing.id)
            assertTrue(runtime.deployments.value.isEmpty())
            assertFailsWith<IllegalArgumentException> { runtime.retryDistroDeployment(existing.id) }
        } finally {
            release.countDown(); server.stop(0); executor.shutdownNow(); runtime.close(); RootfsArchives.deleteTree(directory)
        }
        Unit
    }
}
