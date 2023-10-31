# !/bin/bash

set +ex
username="foo"
server="127.0.0.1"
port=22
know_hosts_strategy="no"
identity_file="/home/foo/.ssh/keys/id_rsa"
while :; do
    (
        sftp -oPort=$port -o StrictHostKeyChecking=$know_hosts_strategy -o IdentityFile=$identity_file $username@$server << EOF
        bye
EOF
    ) 2>/dev/null
    if [[ $? -eq 0 ]]; then
        echo "SFTP is available, proceeding..."
        break
    else
        echo "Waiting for SFTP to be available..."
        sleep 1
    fi
done