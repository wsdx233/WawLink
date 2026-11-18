package dev.wawlink.runtime

/** Bundled POSIX functions shared by toolchain management and the OMP installer. */
object GuestPackageScripts {
    val preamble: String by lazy {
        checkNotNull(javaClass.getResourceAsStream("/wawlink-guest-packages.sh")) { "缺少客体包管理脚本" }
            .bufferedReader(Charsets.UTF_8).use { it.readText() }
    }
}

internal object PackageManagerCommands {
    private fun controlled(command: String): String = """
        (
        ${GuestPackageScripts.preamble}
        wawlink_require_root || exit ${'$'}?
        umask 0022
        $command
        )
    """.trimIndent()

    private fun apt(command: String, prepare: Boolean = false): String = """
        export DEBIAN_FRONTEND=noninteractive
        ${if (prepare) "wawlink_prepare_apt" else "wawlink_repair_dpkg"} && {
        $command
        }
    """.trimIndent()

    fun toolchain(manager: String, packages: String, install: Boolean): String = controlled(when (manager) {
        "apt" -> apt(if (install) "apt-get install -y $packages" else """
            selected=''
            for package in $packages; do
                if dpkg-query -W -f='${'$'}{Status}' "${'$'}package" 2>/dev/null | grep -q '^install ok installed${'$'}'; then selected="${'$'}selected ${'$'}package"; fi
            done
            if [ -n "${'$'}selected" ]; then DEBIAN_FRONTEND=noninteractive apt-get purge -y ${'$'}selected && DEBIAN_FRONTEND=noninteractive apt-get autoremove -y; fi
        """.trimIndent(), prepare = install)
        "apk" -> "apk ${if (install) "add --no-cache" else "del"} $packages"
        "pacman" -> "pacman ${if (install) "-Syu --disable-sandbox --needed" else "-Rns"} --noconfirm $packages"
        "dnf" -> "dnf ${if (install) "install" else "remove"} -y $packages"
        else -> error("不支持的软件包管理器")
    })

    fun clean(manager: String): String = controlled(when (manager) {
        "apt" -> apt("apt-get clean")
        "apk" -> "apk cache clean"
        "pacman" -> "pacman -Scc --noconfirm"
        "dnf" -> "dnf clean all"
        else -> error("不支持的软件包管理器")
    })
}
