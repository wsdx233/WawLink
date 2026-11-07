package dev.wawlink.ui

import androidx.compose.runtime.saveable.SaverScope
import dev.wawlink.protocol.AttachmentFile
import dev.wawlink.protocol.AttachmentSource
import dev.wawlink.protocol.RestoredSessionPrompt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class ComposerDraftRegistryTest {
    private val scope = object : SaverScope {
        override fun canBeSaved(value: Any) = true
    }
    private val local = AttachmentFile("local", "草稿.txt", "/local/draft.txt", "text/plain", 12, AttachmentSource.LOCAL)
    private val recovered = AttachmentFile("recovered", "取回.png", "/workspace/recovered.png", "image/png", 24, AttachmentSource.CONTAINER)

    @Test fun staleSnapshotCannotReplaceLateRecoveryAndKeepsPendingInputLocked() {
        val registry = ComposerDraftRegistry()
        val saver = composerDraftSaver("session", registry)
        val draft = composerDraftForSession("session", registry) { RestoredSessionPrompt("原草稿", listOf(local)) }
        val stale = requireNotNull(with(saver) { scope.save(draft) })
        val persisted = mutableListOf<RestoredSessionPrompt>()
        registry.bindPersistence("session", draft) { persisted.add(it) }

        draft.takingQueuedPrompt = true
        val returnedBeforeRecovery = requireNotNull(saver.restore(stale))
        assertSame(draft, returnedBeforeRecovery)
        assertTrue(returnedBeforeRecovery.takingQueuedPrompt)

        draft.takingQueuedPrompt = false
        val prompt = RestoredSessionPrompt("  未发送原文\n", listOf(recovered))
        draft.restore(prompt, replaceExisting = true)
        val returnedAfterRecovery = requireNotNull(saver.restore(stale))
        assertSame(draft, returnedAfterRecovery)
        assertEquals(prompt, returnedAfterRecovery.snapshot())
        assertEquals(listOf(prompt), persisted)
    }

    @Test fun savingAfterLateRecoveryReadsTheSharedCurrentTextAndAttachments() {
        val registry = ComposerDraftRegistry()
        val saver = composerDraftSaver("session", registry)
        val draft = composerDraftForSession("session", registry) { RestoredSessionPrompt("旧内容", listOf(local)) }
        val prompt = RestoredSessionPrompt("新内容", listOf(recovered, local))
        draft.restore(prompt, replaceExisting = true)

        val saved = requireNotNull(with(saver) { scope.save(draft) })
        val rebuilt = requireNotNull(composerDraftSaver("session", ComposerDraftRegistry()).restore(saved))
        assertEquals(prompt, rebuilt.snapshot())
    }

    @Test fun anExistingEmptyDraftTakesPriorityOverANonemptySnapshot() {
        val registry = ComposerDraftRegistry()
        val saver = composerDraftSaver("session", registry)
        val draft = composerDraftForSession("session", registry) { RestoredSessionPrompt("已经发送", listOf(local)) }
        val stale = requireNotNull(with(saver) { scope.save(draft) })
        draft.textState.value = ""
        draft.attachments.clear()

        assertSame(draft, saver.restore(stale))
        assertEquals(RestoredSessionPrompt(""), draft.snapshot())
    }

    @Test fun sessionIdentitiesAndTheirRecoveryLocksRemainIndependent() {
        val registry = ComposerDraftRegistry()
        val first = composerDraftForSession("first", registry) { RestoredSessionPrompt("第一条", listOf(local)) }
        val second = composerDraftForSession("second", registry) { RestoredSessionPrompt("第二条", listOf(recovered)) }
        val firstSaver = composerDraftSaver("first", registry)
        val firstSnapshot = requireNotNull(with(firstSaver) { scope.save(first) })
        first.takingQueuedPrompt = true

        assertFalse(second.takingQueuedPrompt)
        assertNull(composerDraftSaver("second", registry).restore(firstSnapshot))
        assertEquals(RestoredSessionPrompt("第二条", listOf(recovered)), second.snapshot())
        assertSame(first, composerDraftForSession("first", registry))
    }

    @Test fun aNewHostRestoresContentWithoutAnAbandonedRecoveryLock() {
        val registry = ComposerDraftRegistry()
        val saver = composerDraftSaver("session", registry)
        val draft = composerDraftForSession("session", registry) { RestoredSessionPrompt("保留原文", listOf(local)) }
        draft.takingQueuedPrompt = true
        val saved = requireNotNull(with(saver) { scope.save(draft) })

        val newRegistry = ComposerDraftRegistry()
        val restored = requireNotNull(composerDraftSaver("session", newRegistry).restore(saved))
        assertNotSame(draft, restored)
        assertFalse(restored.takingQueuedPrompt)
        assertEquals(draft.snapshot(), restored.snapshot())
        assertSame(restored, composerDraftForSession("session", newRegistry))
    }

    @Test fun discardedAndClosedEntriesCannotReattachTheirPersistenceCallbacks() {
        val registry = ComposerDraftRegistry()
        val original = composerDraftForSession("session", registry)
        var writes = 0
        registry.bindPersistence("session", original) { writes++ }
        registry.discard("session")
        registry.bindPersistence("session", original) { writes++ }
        original.restore(RestoredSessionPrompt("迟到取回"), replaceExisting = true)
        assertEquals(0, writes)

        val replacement = composerDraftForSession("session", registry)
        assertNotSame(original, replacement)
        registry.bindPersistence("session", replacement) { writes++ }
        registry.close()
        registry.bindPersistence("session", replacement) { writes++ }
        replacement.restore(RestoredSessionPrompt("关闭后取回"), replaceExisting = true)
        assertEquals(0, writes)
        val afterClose = composerDraftForSession("session", registry)
        assertNotSame(afterClose, composerDraftForSession("session", registry))
    }
}
