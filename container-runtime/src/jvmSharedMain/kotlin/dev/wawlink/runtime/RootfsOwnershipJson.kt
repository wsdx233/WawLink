package dev.wawlink.runtime

import com.google.gson.Strictness
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.google.gson.stream.JsonWriter
import java.io.InputStream
import java.io.OutputStream

/** Only full migration/archive reads retain entries; ordinary queries and edits stream past them. */
internal object RootfsOwnershipJson {
    data class Status(val version: Int, val physical: ContainerEngineType, val origin: String, val pending: Boolean)
    data class Document(val status: Status, val entries: Map<String, LogicalOwner>?, val migration: OwnershipMigration?, val owner: LogicalOwner?)

    fun validPath(path: String): Boolean {
        if (path.isEmpty() || path[0] != '/') return false
        if (path.length == 1) return true
        var start = 1
        for (index in 1..path.length) {
            if (index < path.length && path[index] != '/') {
                if (path[index] == '\u0000' || path[index] == '\n' || path[index] == '\r') return false
                continue
            }
            val length = index - start
            if (length == 0 || length == 1 && path[start] == '.' || length == 2 && path[start] == '.' && path[start + 1] == '.') return false
            start = index + 1
        }
        return true
    }

    private fun checkOwner(path: String, uid: Long, gid: Long, mode: Long) {
        require(validPath(path) && uid in 0..4294967294L && gid in 0..4294967294L && mode in 0..4095L) {
            "逻辑属主清单包含无效路径、身份或权限位：$path"
        }
    }

    private fun writer(output: OutputStream) = JsonWriter(output.writer(Charsets.UTF_8).buffered()).apply {
        strictness = Strictness.STRICT
        serializeNulls = true
    }

    private fun JsonWriter.owner(uid: Long, gid: Long, mode: Long) {
        beginObject()
        name("uid").value(uid)
        name("gid").value(gid)
        name("mode").value(mode)
        endObject()
    }

    fun write(output: OutputStream, manifest: OwnershipManifest) {
        require(manifest.version == 1) { "不支持的逻辑属主清单版本：${manifest.version}" }
        val writer = writer(output)
        writer.beginObject().name("entries").beginObject()
        for ((path, owner) in manifest.entries) {
            checkOwner(path, owner.uid, owner.gid, owner.mode.toLong())
            writer.name(path).owner(owner.uid, owner.gid, owner.mode.toLong())
        }
        writer.endObject()
        writer.name("version").value(manifest.version.toLong())
        writer.name("physical").value(manifest.physical.name)
        writer.name("origin").value(manifest.origin)
        writer.name("pending")
        val pending = manifest.pending
        if (pending == null) writer.nullValue() else {
            writer.beginObject()
            writer.name("from").value(pending.from.name)
            writer.name("to").value(pending.to.name)
            writer.name("identities").beginObject()
            for ((path, identity) in pending.identities) writer.name(path).value(identity)
            writer.endObject().endObject()
        }
        writer.endObject().flush()
    }

    /** Rewrites one owner without retaining either the entries or the pending inode journal. */
    fun read(input: InputStream, retainEntries: Boolean = false, guestPath: String? = null,
             output: OutputStream? = null, replacement: LogicalOwner? = null): Document {
        require(output == null || guestPath != null && replacement != null)
        if (replacement != null) checkOwner(requireNotNull(guestPath), replacement.uid, replacement.gid, replacement.mode.toLong())
        val entries = if (retainEntries) LinkedHashMap<String, LogicalOwner>() else null
        val writer = output?.let(::writer)
        var version = 1
        var physical = ContainerEngineType.PROOT
        var origin = "archive"
        var migration: OwnershipMigration? = null
        var owner: LogicalOwner? = null
        var hasEntries = false
        JsonReader(input.reader(Charsets.UTF_8)).use { reader ->
            reader.strictness = Strictness.STRICT
            reader.beginObject()
            writer?.beginObject()
            while (reader.hasNext()) {
                when (val name = reader.nextName()) {
                    "entries" -> {
                        hasEntries = true
                        entries?.clear()
                        owner = null
                        reader.beginObject()
                        writer?.name(name)?.beginObject()
                        while (reader.hasNext()) {
                            val path = reader.nextName()
                            var uid = Long.MIN_VALUE
                            var gid = Long.MIN_VALUE
                            var mode = Long.MIN_VALUE
                            reader.beginObject()
                            while (reader.hasNext()) when (reader.nextName()) {
                                "uid" -> uid = reader.nextLong()
                                "gid" -> gid = reader.nextLong()
                                "mode" -> mode = reader.nextLong()
                                else -> reader.skipValue()
                            }
                            reader.endObject()
                            checkOwner(path, uid, gid, mode)
                            if (entries != null || path == guestPath) {
                                val logical = LogicalOwner(uid, gid, mode.toInt())
                                entries?.put(path, logical)
                                if (path == guestPath) owner = logical
                            }
                            if (writer != null) {
                                writer.name(path)
                                if (path == guestPath) {
                                    val changed = requireNotNull(replacement)
                                    writer.owner(changed.uid, changed.gid, changed.mode.toLong())
                                } else writer.owner(uid, gid, mode)
                            }
                        }
                        reader.endObject()
                        if (writer != null && owner == null) {
                            val added = requireNotNull(replacement)
                            writer.name(requireNotNull(guestPath)).owner(added.uid, added.gid, added.mode.toLong())
                        }
                        writer?.endObject()
                    }
                    "version" -> {
                        val value = reader.nextLong()
                        require(value == 1L) { "不支持的逻辑属主清单版本：$value" }
                        version = value.toInt()
                        writer?.name(name)?.value(value)
                    }
                    "physical" -> {
                        physical = ContainerEngineType.valueOf(reader.nextString())
                        writer?.name(name)?.value(physical.name)
                    }
                    "origin" -> {
                        origin = reader.nextString()
                        writer?.name(name)?.value(origin)
                    }
                    "pending" -> {
                        writer?.name(name)
                        migration = readMigration(reader, writer, retainEntries)
                    }
                    else -> reader.skipValue()
                }
            }
            reader.endObject()
            require(hasEntries && reader.peek() == JsonToken.END_DOCUMENT) { "逻辑属主清单不完整或包含多余内容" }
            writer?.endObject()?.flush()
        }
        return Document(Status(version, physical, origin, migration != null), entries, migration, owner)
    }

    private fun readMigration(reader: JsonReader, writer: JsonWriter?, retain: Boolean): OwnershipMigration? {
        if (reader.peek() == JsonToken.NULL) {
            reader.nextNull()
            writer?.nullValue()
            return null
        }
        var from: ContainerEngineType? = null
        var to: ContainerEngineType? = null
        val identities = if (retain) LinkedHashMap<String, String>() else null
        reader.beginObject()
        writer?.beginObject()
        while (reader.hasNext()) when (val name = reader.nextName()) {
            "from" -> {
                from = ContainerEngineType.valueOf(reader.nextString())
                writer?.name(name)?.value(from.name)
            }
            "to" -> {
                to = ContainerEngineType.valueOf(reader.nextString())
                writer?.name(name)?.value(to.name)
            }
            "identities" -> {
                reader.beginObject()
                identities?.clear()
                writer?.name(name)?.beginObject()
                while (reader.hasNext()) {
                    val path = reader.nextName()
                    val identity = reader.nextString()
                    identities?.put(path, identity)
                    writer?.name(path)?.value(identity)
                }
                reader.endObject()
                writer?.endObject()
            }
            else -> reader.skipValue()
        }
        reader.endObject()
        writer?.endObject()
        return OwnershipMigration(requireNotNull(from) { "迁移日志缺少来源引擎" }, requireNotNull(to) { "迁移日志缺少目标引擎" }, identities ?: emptyMap())
    }
}
