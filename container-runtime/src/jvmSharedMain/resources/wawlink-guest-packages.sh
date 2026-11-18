# Shared POSIX guest package operations. Never source host credentials or escalate.
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
wawlink_require_root() {
    [ "$(id -u)" = 0 ] || { printf '%s\n' 'ERROR: Package operations require guest root; select the container root user. No privilege escalation is attempted.' >&2; return 1; }
}
wawlink_repair_dpkg() {
    DEBIAN_FRONTEND=noninteractive dpkg --configure -a
}
wawlink_apt_update() {
    apt-get -o APT::Update::Error-Mode=any -o Acquire::Retries=2 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update
}
# Minimal images may lack CA roots. Retain repository signatures/package hashes
# and source selection; only temporary source copies use HTTP for this bootstrap.
wawlink_bootstrap_apt_certificates() (
    ca_stage=$(mktemp -d /tmp/wawlink-apt-ca.XXXXXX) || exit $?
    trap 'rm -rf -- "$ca_stage"' 0
    trap 'exit 130' INT
    trap 'exit 143' TERM HUP
    mkdir "$ca_stage/sources.list.d" || exit $?
    : > "$ca_stage/sources.list" || exit $?
    for source in /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
        [ -f "$source" ] || continue
        case "$source" in
            /etc/apt/sources.list) target="$ca_stage/sources.list" ;;
            *) target="$ca_stage/sources.list.d/${source##*/}" ;;
        esac
        sed 's|https://|http://|g' "$source" > "$target" || exit $?
    done
    set -- -o "Dir::Etc::sourcelist=$ca_stage/sources.list" -o "Dir::Etc::sourceparts=$ca_stage/sources.list.d" -o APT::Update::Error-Mode=any -o Acquire::Retries=2 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30
    apt-get "$@" update && apt-get "$@" install -y --no-install-recommends ca-certificates
)
wawlink_prepare_apt() {
    export DEBIAN_FRONTEND=noninteractive
    wawlink_repair_dpkg || return $?
    if [ ! -s /etc/ssl/certs/ca-certificates.crt ]; then
        wawlink_bootstrap_apt_certificates || return $?
    fi
    wawlink_apt_update
}
