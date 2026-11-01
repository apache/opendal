#!/bin/sh
set -eu
task_delay=$(cat /task-state/delay)
case "$task_delay" in 1|12) ;; *) exit 2 ;; esac
printf 'TASK_AUTH_BEGIN delay=%s epoch=%s\n' "$task_delay" "$(date -u +%s)" >&2
sleep "$task_delay"
printf 'TASK_AUTH_REJECT delay=%s epoch=%s\n' "$task_delay" "$(date -u +%s)" >&2
exit 1
