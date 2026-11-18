package dev.wawlink.runtime

import java.io.File
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PackageManagerCommandsTest {
    private fun fixture(block: (File, File) -> Unit) {
        val directory = Files.createTempDirectory("wawlink-package-manager-").toFile()
        val tools = File(directory, "tools").apply { mkdirs() }
        executable(tools, "id", "printf '0\\n'")
        File(directory, "ca.crt").writeText("fixture trust store")
        File(directory, "apt/sources.list.d").mkdirs()
        try { block(directory, tools) } finally { RootfsArchives.deleteTree(directory) }
    }

    private fun executable(directory: File, name: String, body: String): File = File(directory, name).apply {
        writeText("#!/bin/sh\n$body\n")
        setExecutable(true)
    }

    private fun run(command: String, tools: File, log: File, dpkgStatus: Int = 0): Int {
        // Production never accepts a caller-controlled PATH. Rewrite only this
        // test copy so every package command is a local, non-installing stub.
        val fixtureScript = command
            .replace("export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "export PATH=${tools.path}:/usr/bin:/bin")
            .replace("/etc/ssl/certs/ca-certificates.crt", File(tools.parentFile, "ca.crt").path)
            .replace("/etc/apt", File(tools.parentFile, "apt").path)
        val process = ProcessBuilder("/bin/sh", "-c", fixtureScript).apply {
            environment()["WAWLINK_PACKAGE_LOG"] = log.path
            environment()["WAWLINK_DPKG_STATUS"] = dpkgStatus.toString()
        }.start()
        process.inputStream.use { it.readBytes() }
        process.errorStream.use { it.readBytes() }
        process.waitFor()
        return process.exitValue()
    }

    @Test fun aptToolchainInstallRepairsDpkgBeforeUpdateAndInstall() = fixture { _, tools ->
        val log = File(tools.parentFile, "commands.log")
        executable(tools, "dpkg", "printf 'dpkg:%s:%s\\n' \"${'$'}DEBIAN_FRONTEND\" \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"\nexit \"${'$'}WAWLINK_DPKG_STATUS\"")
        executable(tools, "apt-get", "printf 'apt:%s:%s\\n' \"${'$'}DEBIAN_FRONTEND\" \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"")

        val exit = run(PackageManagerCommands.toolchain("apt", "git", install = true), tools, log)

        assertEquals(0, exit)
        assertEquals(
            listOf(
                "dpkg:noninteractive:--configure -a",
                "apt:noninteractive:-o APT::Update::Error-Mode=any -o Acquire::Retries=2 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update",
                "apt:noninteractive:install -y git",
            ),
            log.readLines(),
        )
    }

    @Test fun failedDpkgRepairStopsAptWithoutMaskingItsExitStatus() = fixture { _, tools ->
        val log = File(tools.parentFile, "commands.log")
        executable(tools, "dpkg", "printf 'dpkg\\n' >> \"${'$'}WAWLINK_PACKAGE_LOG\"\nexit \"${'$'}WAWLINK_DPKG_STATUS\"")
        executable(tools, "apt-get", "printf 'apt\\n' >> \"${'$'}WAWLINK_PACKAGE_LOG\"")

        val exit = run(PackageManagerCommands.toolchain("apt", "git", install = true), tools, log, dpkgStatus = 42)

        assertEquals(42, exit)
        assertEquals(listOf("dpkg"), log.readLines())
    }

    @Test fun aptRemovalAndCacheCleanAlsoRepairDpkgFirst() = fixture { _, tools ->
        val log = File(tools.parentFile, "commands.log")
        executable(tools, "dpkg", "printf 'dpkg:%s\\n' \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"")
        executable(tools, "dpkg-query", "printf 'install ok installed\\n'")
        executable(tools, "apt-get", "printf 'apt:%s\\n' \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"")

        assertEquals(0, run(PackageManagerCommands.toolchain("apt", "git", install = false), tools, log))
        assertEquals(
            listOf(
                "dpkg:--configure -a",
                "apt:purge -y git",
                "apt:autoremove -y",
            ),
            log.readLines(),
        )

        log.writeText("")
        assertEquals(0, run(PackageManagerCommands.clean("apt"), tools, log))
        assertEquals(listOf("dpkg:--configure -a", "apt:clean"), log.readLines())
    }

    @Test fun strictUpdateFailurePreventsInstall() = fixture { _, tools ->
        val log = File(tools.parentFile, "commands.log")
        executable(tools, "dpkg", "exit 0")
        executable(tools, "apt-get", "printf '%s\\n' \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"\nexit 43")
        assertEquals(43, run(PackageManagerCommands.toolchain("apt", "git", true), tools, log))
        assertEquals(1, log.readLines().size)
        assertTrue(log.readText().contains("APT::Update::Error-Mode=any"))
    }

    @Test fun missingCaUsesSignedTemporarySourcesThenOriginalSources() = fixture { directory, tools ->
        val log = File(directory, "commands.log")
        File(directory, "ca.crt").delete()
        val source = File(directory, "apt/sources.list").apply { writeText("deb https://mirror.example/debian stable main\n") }
        executable(tools, "dpkg", "exit 0")
        executable(tools, "apt-get", """
            printf '%s\n' "${'$'}*" >> "${'$'}WAWLINK_PACKAGE_LOG"
            for argument do
                case "${'$'}argument" in
                    Dir::Etc::sourcelist=*) cat "${'$'}{argument#*=}" >> "${'$'}WAWLINK_PACKAGE_LOG" ;;
                esac
            done
        """.trimIndent())
        assertEquals(0, run(PackageManagerCommands.toolchain("apt", "git", true), tools, log))
        val lines = log.readLines()
        assertEquals(6, lines.size)
        assertTrue(lines[0].contains("Dir::Etc::sourcelist="))
        assertTrue(lines[0].contains("APT::Update::Error-Mode=any"))
        assertEquals("deb http://mirror.example/debian stable main", lines[1])
        assertTrue(lines[2].endsWith("install -y --no-install-recommends ca-certificates"))
        assertTrue(lines[4].endsWith(" update"))
        assertEquals("install -y git", lines[5])
        assertEquals("deb https://mirror.example/debian stable main\n", source.readText())
        assertTrue(!log.readText().contains("AllowUnauthenticated"))
    }

    @Test fun failedSignedBootstrapStopsAndRemovesTemporarySources() = fixture { directory, tools ->
        val log = File(directory, "commands.log")
        File(directory, "ca.crt").delete()
        executable(tools, "dpkg", "exit 0")
        executable(tools, "apt-get", "printf '%s\\n' \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"\nexit 61")
        assertEquals(61, run(PackageManagerCommands.toolchain("apt", "git", true), tools, log))
        assertEquals(1, log.readLines().size)
        val source = Regex("Dir::Etc::sourcelist=([^ ]+)").find(log.readText())!!.groupValues[1]
        assertTrue(!File(source).parentFile.exists())
        assertTrue(!log.readText().contains("install"))
    }

    @Test fun cacheCleanWithMissingCaNeverBootstrapsOrUpdates() = fixture { directory, tools ->
        File(directory, "ca.crt").delete()
        val log = File(directory, "commands.log")
        executable(tools, "dpkg", "printf 'repair\\n' >> \"${'$'}WAWLINK_PACKAGE_LOG\"")
        executable(tools, "apt-get", "printf '%s\\n' \"${'$'}*\" >> \"${'$'}WAWLINK_PACKAGE_LOG\"")
        assertEquals(0, run(PackageManagerCommands.clean("apt"), tools, log))
        assertEquals(listOf("repair", "clean"), log.readLines())
    }

    @Test fun nonRootCannotReachPackageManager() = fixture { directory, tools ->
        val log = File(directory, "commands.log")
        executable(tools, "id", "printf '1000\\n'")
        for (manager in listOf("apt", "apk", "pacman", "dnf")) {
            assertEquals(1, run(PackageManagerCommands.toolchain(manager, "git", true), tools, log))
            assertEquals(1, run(PackageManagerCommands.clean(manager), tools, log))
        }
        assertTrue(!log.exists())
    }
}
