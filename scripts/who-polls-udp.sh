#!/usr/bin/env bash
set -euo pipefail

pid=${1:?usage: $0 <pid> <port>}
port=${2:?usage: $0 <pid> <port>}
proc=/proc/$pid

mapfile -t sockets < <(ss -uanp 2>/dev/null |
    grep "pid=$pid," |
    awk -v port=":$port" '$4 ~ port"$" { match($0, /fd=[0-9]+/); print substr($0, RSTART + 3, RLENGTH - 3), $4 }')

if [[ ${#sockets[@]} -eq 0 ]]; then
    echo "pid $pid has no UDP socket on port $port"
    exit 1
fi

declare -A epoll_fds_by_watchlist
for fd_path in "$proc"/fd/*; do
    [[ $(readlink "$fd_path" 2>/dev/null) == "anon_inode:[eventpoll]" ]] || continue
    fd=${fd_path##*/}
    watchlist=" $(awk '/^tfd:/ {print $2}' "$proc/fdinfo/$fd" | sort -n | tr '\n' ' ')"
    epoll_fds_by_watchlist[$watchlist]+="$fd "
done

for socket in "${sockets[@]}"; do
    read -r socket_fd local_addr <<< "$socket"
    watchers=()
    others=()
    for watchlist in "${!epoll_fds_by_watchlist[@]}"; do
        epoll="[ ${epoll_fds_by_watchlist[$watchlist]}]"
        if [[ $watchlist == *" $socket_fd "* ]]; then
            watchers+=("$epoll")
        else
            others+=("$epoll")
        fi
    done

    echo "udp $local_addr is fd $socket_fd of pid $pid"
    if [[ ${#watchers[@]} -gt 0 ]]; then
        echo "  watched by epoll fds ${watchers[*]}"
    else
        echo "  watched by no epoll instance"
    fi
    echo "  not watched by the other ${#others[@]} epoll instance(s): ${others[*]}"
done
