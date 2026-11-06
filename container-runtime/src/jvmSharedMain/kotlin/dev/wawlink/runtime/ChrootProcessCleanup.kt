package dev.wawlink.runtime

import java.io.File

/** Stop-time safety net for double-forked/reparented guests. Never inspects shared device FDs.
 * Full /proc scans happen only between bounded signal/wait phases, not in the runtime poller.
 */
internal object ChrootProcessCleanup {
    fun script(root: File, procRoot: File = File("/proc"), termPolls: Int = 30, killPolls: Int = 20): String {
        require(root.isAbsolute && root.path != "/" && '\u0000' !in root.path)
        require(procRoot.isAbsolute && '\u0000' !in procRoot.path)
        require(termPolls in 1..100 && killPolls in 1..100)
        return """
            # wawlink-chroot-process-cleanup
            set -eu
            set -f
            root=${shellQuote(root.path.trimEnd('/'))}
            proc=${shellQuote(procRoot.path)}
            fail() { printf '%s\n' "无法安全回收 chroot 进程：${'$'}*" >&2; exit 1; }
            [ -r "${'$'}proc/self/stat" ] && [ -r "${'$'}proc" ] && [ -x "${'$'}proc" ] || fail 'Root /proc 不可读取'
            read_stat() {
                row=${'$'}(cat "${'$'}proc/${'$'}pid/stat" 2>/dev/null) || {
                    [ ! -d "${'$'}proc/${'$'}pid" ] && return 1
                    fail "stat 不可读取 PID ${'$'}pid"
                }
                if [ -z "${'$'}row" ]; then
                    [ ! -d "${'$'}proc/${'$'}pid" ] && return 1
                    fail "stat 为空 PID ${'$'}pid"
                fi
                case "${'$'}row" in "${'$'}pid ("*) ;; *) fail "stat PID 无效 ${'$'}pid" ;; esac
                rest=${'$'}{row##*) }
                set -- ${'$'}rest
                [ "${'$'}#" -ge 20 ] || fail "stat 格式无效 PID ${'$'}pid"
                state=${'$'}1
                shift 19
                start=${'$'}1
                case "${'$'}start" in ''|*[!0-9]*) fail "stat starttime 无效 PID ${'$'}pid" ;; esac
                [ "${'$'}state" != Z ] && [ "${'$'}state" != X ] || return 1
            }
            read_root() {
                # A sentinel preserves literal trailing newlines through command substitution.
                actual=${'$'}(readlink "${'$'}proc/${'$'}pid/root" 2>/dev/null && printf '.') || return 1
                actual=${'$'}{actual%.}
                # readlink appends exactly one newline; do not strip path whitespace/backslashes.
                actual=${'$'}{actual%?}
                case "${'$'}actual" in "${'$'}root"|"${'$'}root"/*) return 0 ;; *) return 1 ;; esac
            }
            scan() {
                candidates=''
                set +f
                for entry in "${'$'}proc"/[0-9]*; do
                    set -f
                    [ -d "${'$'}entry" ] || continue
                    pid=${'$'}{entry##*/}
                    case "${'$'}pid" in *[!0-9]*|'') continue ;; esac
                    # Unknown/inaccessible host roots never authorize a signal. Once a
                    # root matches, identity and root read failures are real stop failures.
                    read_root || continue
                    read_stat || continue
                    identity="${'$'}pid:${'$'}start"
                    matches || continue
                    candidates="${'$'}candidates ${'$'}pid:${'$'}start"
                done
                set -f
            }
            matches() {
                pid=${'$'}{identity%%:*}
                expected=${'$'}{identity#*:}
                read_stat || return 1
                [ "${'$'}start" = "${'$'}expected" ] || fail "PID 身份已变化 ${'$'}pid"
                read_root || {
                    read_stat || return 1
                    fail "root 已变化或不可读取（Root/SELinux）PID ${'$'}pid"
                }
                read_stat || return 1
                [ "${'$'}start" = "${'$'}expected" ] || fail "PID 身份已变化 ${'$'}pid"
            }
            signal_all() {
                for identity in ${'$'}candidates; do
                    matches || continue
                    kill -"${'$'}signal" "${'$'}pid" 2>/dev/null || {
                        matches || continue
                        fail "${'$'}signal 失败 PID ${'$'}pid"
                    }
                done
            }
            wait_all() {
                remaining=${'$'}1
                while :; do
                    live=''
                    for identity in ${'$'}candidates; do
                        matches && live="${'$'}live ${'$'}identity"
                    done
                    candidates=${'$'}live
                    [ -n "${'$'}candidates" ] || return 0
                    [ "${'$'}remaining" -gt 0 ] || return 1
                    sleep 0.1
                    remaining=${'$'}((remaining - 1))
                done
            }
            # A guest may fork during TERM; rescan only at phase boundaries. Persistent
            # forking or inaccessible identities fail closed and leave mounts retryable.
            round=0
            while [ "${'$'}round" -lt 3 ]; do
                scan
                [ -n "${'$'}candidates" ] || exit 0
                signal=TERM; signal_all
                wait_all $termPolls || true
                signal=KILL; signal_all
                wait_all $killPolls || fail "KILL 后进程仍未退出 ${'$'}candidates"
                round=${'$'}((round + 1))
            done
            scan
            [ -z "${'$'}candidates" ] || fail "仍有新生进程 ${'$'}candidates"
        """.trimIndent()
    }
}
