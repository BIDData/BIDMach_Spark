#!/bin/bash
while read wip; do
  local_ip=$(dig +short ${wip})
  echo "$local_ip ($wip)"
  ssh -T "ec2-user@${local_ip}" << EOS
$@
EOS
done < /opt/spark/conf/slaves
echo 'done'
