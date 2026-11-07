package dev.wawlink.app

import dev.wawlink.protocol.*
import java.nio.file.Files
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.*
import kotlinx.coroutines.*

class SessionDraftStoreTest {
    @Test fun slowDiskNeverBlocksTypingAndCloseFlushesTheLatestRevision(): Unit = runBlocking {
        val directory = Files.createTempDirectory("wawlink-drafts-slow-").toFile()
        val errors = CopyOnWriteArrayList<Throwable>()
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val first = AtomicBoolean(true)
        val store = SessionDraftStore(directory, { true }, errors::add, writeFile = { file, text ->
            if (first.getAndSet(false)) { started.countDown(); check(release.await(10, TimeUnit.SECONDS)) }
            file.writeText(text)
        })
        try {
            store.update("existing", RestoredSessionPrompt("旧草稿"))
            assertTrue(withContext(Dispatchers.IO) { started.await(5, TimeUnit.SECONDS) })
            val latest = RestoredSessionPrompt("继续输入的新草稿\n ")
            withTimeout(2_000) { withContext(Dispatchers.Default) { store.update("existing", latest) } }
            assertEquals(latest, withTimeout(2_000) { store.load("existing") })
            val closing = async(Dispatchers.IO) { store.close() }
            release.countDown()
            withTimeout(5_000) { closing.await() }
            SessionDraftStore(directory, { true }, errors::add).use { assertEquals(latest, it.load("existing")) }
            assertTrue(errors.isEmpty())
        } finally {
            release.countDown(); store.close(); directory.deleteRecursively()
        }
    }

    @Test fun discardingDuringAnInflightWriteCannotResurrectTheDraft(): Unit = runBlocking {
        val directory = Files.createTempDirectory("wawlink-drafts-delete-").toFile()
        val errors = CopyOnWriteArrayList<Throwable>()
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val store = SessionDraftStore(directory, { true }, errors::add, writeFile = { file, text ->
            started.countDown(); check(release.await(10, TimeUnit.SECONDS)); file.writeText(text)
        })
        try {
            store.update("deleted", RestoredSessionPrompt("写入中的草稿"))
            assertTrue(withContext(Dispatchers.IO) { started.await(5, TimeUnit.SECONDS) })
            withTimeout(2_000) { withContext(Dispatchers.Default) { store.discard("deleted") } }
            store.update("deleted", RestoredSessionPrompt("迟到更新"))
            assertNull(store.load("deleted"))
            val closing = async(Dispatchers.IO) { store.close() }
            release.countDown()
            withTimeout(5_000) { closing.await() }
            SessionDraftStore(directory, { true }, errors::add).use { assertNull(it.load("deleted")) }
            assertTrue(directory.resolve("drafts").listFiles().orEmpty().isEmpty())
            assertTrue(errors.isEmpty())
        } finally {
            release.countDown(); store.close(); directory.deleteRecursively()
        }
    }

    @Test fun restartRestoresExactTextAndAttachmentIdentityAndClearedDraftStaysEmpty(): Unit = runBlocking {
        val directory = Files.createTempDirectory("wawlink-drafts-").toFile()
        val errors = mutableListOf<Throwable>()
        try {
            val prompt = RestoredSessionPrompt("  原文\n\n ", listOf(AttachmentFile("image", "图片.png", "/workspace/图片.png", "image/png", 18, AttachmentSource.CONTAINER)))
            SessionDraftStore(directory, { true }, errors::add).use { store -> store.update("existing", prompt) }
            SessionDraftStore(directory, { true }, errors::add).use { store ->
                assertEquals(prompt, store.load("existing"))
                store.update("existing", RestoredSessionPrompt(""))
            }
            SessionDraftStore(directory, { true }, errors::add).use { assertNull(it.load("existing")) }
            assertTrue(errors.isEmpty())
        } finally { directory.deleteRecursively() }
    }

    @Test fun provisionalDraftsNeverBecomePersistedSessionsAndDeletedDraftsCannotReappear(): Unit = runBlocking {
        val directory = Files.createTempDirectory("wawlink-drafts-").toFile()
        try {
            SessionDraftStore(directory, { it == "durable" }, { throw it }).use { store ->
                store.update("provisional", RestoredSessionPrompt("不落盘"))
                store.update("durable", RestoredSessionPrompt("旧草稿"))
                store.discard("durable")
                store.update("durable", RestoredSessionPrompt("迟到更新"))
            }
            SessionDraftStore(directory, { true }, { throw it }).use { store ->
                assertNull(store.load("provisional"))
                assertNull(store.load("durable"))
            }
        } finally { directory.deleteRecursively() }
    }
}
