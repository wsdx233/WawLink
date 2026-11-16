package dev.wawlink.runtime

import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption.NOFOLLOW_LINKS
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RootfsOwnershipStoreTest {
    private fun fixture(block: (File, ContainerConfig) -> Unit) {
        val directory = Files.createTempDirectory("wawlink-ownership-store-").toFile()
        val root = File(directory, "root").apply { mkdir() }
        try { block(root, ContainerConfig("store", "Store", root.path)) }
        finally { RootfsArchives.deleteTree(directory) }
    }

    @Test fun legacyManifestAndSingleOwnerEditsPreservePortableIdsModesAndPendingInodes() = fixture { root, config ->
        val entries = linkedMapOf(
            "/" to LogicalOwner(0, 0, 0x1ED),
            "/home/中文/quote\" and \\name" to LogicalOwner(4294967294L, 4294967293L, 0xFFF),
            "/untouched" to LogicalOwner(1001, 1002, 0x9A0),
        )
        val pending = OwnershipMigration(ContainerEngineType.PROOT, ContainerEngineType.CHROOT,
            mapOf("/" to "7:8:1:16384", "/home/中文/quote\" and \\name" to "7:9:1:32768"))
        val original = OwnershipManifest(entries, physical = ContainerEngineType.PROOT, origin = "archive", pending = pending)
        // This is the format written by released versions, not the streaming writer under test.
        RootfsOwnership.sidecar(root).writeText(Json.encodeToString(original))
        assertEquals(original, RootfsOwnership.load(root))
        assertFailsWith<IllegalStateException> { RootfsOwnership.requireStable(config) }
        val changed = LogicalOwner(1010, 2020, 0xD80)
        val added = LogicalOwner(3030, 4040, 0x3A0)
        val path = "/home/中文/quote\" and \\name"
        RootfsOwnership.recordCommittedHostWrite(config, root.toPath(), File(root, path.removePrefix("/")).toPath(), changed)
        RootfsOwnership.recordCommittedHostWrite(config, root.toPath(), File(root, "new file").toPath(), added)
        val expected = original.copy(entries = entries + (path to changed) + ("/new file" to added))
        assertEquals(expected, RootfsOwnership.load(root))
        assertEquals(expected, Json.decodeFromString<OwnershipManifest>(RootfsOwnership.sidecar(root).readText()))
        assertEquals(changed.copy(mode = 0x1A0), RootfsOwnership.managedOwner(config, path, 0x1A0, directory = false))
        assertEquals(0x180, (Files.getAttribute(RootfsOwnership.sidecar(root).toPath(), "unix:mode", NOFOLLOW_LINKS) as Number).toInt() and 0xFFF)
        val restored = File(root.parentFile, "restored")
        RootfsOwnership.copyManifest(root, restored)
        assertEquals(expected, RootfsOwnership.load(restored))
    }

    @Test fun warmedStatusRejectsSameSizeExternalChangesAndAtomicReplacementJournals() = fixture { root, config ->
        val manifest = OwnershipManifest(mapOf("/" to LogicalOwner(0, 0, 0x1ED)))
        RootfsOwnership.save(root, manifest)
        assertFalse(requireNotNull(RootfsOwnership.status(root)).pending)
        RootfsOwnership.requireStable(config)
        val sidecar = RootfsOwnership.sidecar(root).toPath()
        val modified = Files.getLastModifiedTime(sidecar)
        val bytes = Files.readAllBytes(sidecar)
        // Cross the filesystem's actual ctime tick; repeated same-tick writes can share all metadata.
        val changed = Files.getAttribute(sidecar, "unix:ctime", NOFOLLOW_LINKS)
        val invalid = bytes.toString(Charsets.UTF_8).replace("PROOT", "BOGUS").toByteArray(Charsets.UTF_8)
        val deadline = System.nanoTime() + 5_000_000_000L
        do {
            Files.write(sidecar, invalid)
            Files.setLastModifiedTime(sidecar, modified)
            if (Files.getAttribute(sidecar, "unix:ctime", NOFOLLOW_LINKS) != changed) break
            check(System.nanoTime() < deadline) { "文件系统 ctime 未更新" }
            Thread.sleep(1)
        } while (true)
        assertFails { RootfsOwnership.requireStable(config) }
        val interrupted = manifest.copy(physical = ContainerEngineType.CHROOT,
            pending = OwnershipMigration(ContainerEngineType.CHROOT, ContainerEngineType.PROOT, mapOf("/" to "1:2:1:16384")))
        val replacement = Files.createTempFile(sidecar.parent, ".replacement-", ".json")
        Files.writeString(replacement, Json.encodeToString(interrupted))
        Files.move(replacement, sidecar, ATOMIC_MOVE, REPLACE_EXISTING)
        assertTrue(requireNotNull(RootfsOwnership.status(root)).pending)
        assertEquals(ContainerEngineType.CHROOT, RootfsOwnership.physical(config))
        assertFailsWith<IllegalStateException> { RootfsOwnership.requireStable(config) }
        RootfsOwnership.save(root, manifest)
        assertFalse(requireNotNull(RootfsOwnership.status(root)).pending)
        RootfsOwnership.requireStable(config)
        Files.delete(sidecar)
        assertNull(RootfsOwnership.status(root))
    }

    @Test fun malformedOrUnsafeEntriesNeverCommitAPartialOwnerUpdate() = fixture { root, config ->
        val file = RootfsOwnership.sidecar(root)
        for (invalid in listOf(
            "{\"entries\":{\"/ok\":{\"uid\":0,\"gid\":0,\"mode\":384},\"/../escape\":{\"uid\":0,\"gid\":0,\"mode\":384}}}",
            "{\"entries\":{\"/ok\":{\"uid\":4294967295,\"gid\":0,\"mode\":384}}}",
            "{\"entries\":{\"/ok\":{\"uid\":0,\"gid\":0,\"mode\":4096}}}",
            "{\"entries\":{\"/ok\":{\"uid\":0,\"mode\":384}}}",
            "{\"entries\":{},\"version\":2}",
            "{\"entries\":{},\"pending\":{\"from\":\"PROOT\"}}",
            "{\"entries\":{\"/ok\":{\"uid\":0,\"gid\":0,\"mode\":384}},",
        )) {
            file.writeText(invalid)
            assertFails { RootfsOwnership.status(root) }
            assertFails { RootfsOwnership.load(root) }
            assertFails { RootfsOwnership.recordCommittedHostWrite(config, root.toPath(), File(root, "ok").toPath(), LogicalOwner(10, 20, 384)) }
            assertEquals(invalid, file.readText())
            assertTrue(root.parentFile.listFiles().orEmpty().none { it.name.startsWith(".ownership-") })
        }
    }

    @Test fun cachedStatusDoesNotAuthorizeLinkedSidecars() = fixture { root, config ->
        val manifest = OwnershipManifest(mapOf("/" to LogicalOwner(0, 0, 0x1ED)))
        RootfsOwnership.save(root, manifest)
        RootfsOwnership.requireStable(config)
        val sidecar = RootfsOwnership.sidecar(root).toPath()
        val alias = File(root.parentFile, "alias").toPath()
        Files.createLink(alias, sidecar)
        assertFailsWith<IllegalArgumentException> { RootfsOwnership.requireStable(config) }
        Files.delete(alias)
        val original = File(root.parentFile, "original").toPath()
        Files.move(sidecar, original)
        Files.createSymbolicLink(sidecar, original)
        assertFailsWith<IllegalArgumentException> { RootfsOwnership.status(root) }
    }
}
