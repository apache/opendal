#!/bin/sh
set -eu
task_delay=$(cat /task-state/delay)
case "$task_delay" in 1|12) ;; *) exit 2 ;; esac
printf 'TASK_AUTH_BEGIN delay=%s epoch=%s\n' "$task_delay" "$(date -u +%s)" >>/tmp/pr7424-auth.events
sleep "$task_delay"
printf 'TASK_AUTH_REJECT delay=%s epoch=%s\n' "$task_delay" "$(date -u +%s)" >>/tmp/pr7424-auth.events
exit 1
