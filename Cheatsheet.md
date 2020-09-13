# Cheatsheet

### Reset offset for a group on a topic to earliest message.
```
kafka-consumer-groups --bootstrap-server localhost:9092 --group <group-application> --topic <topic-name> --reset-offsets --to-earliest --execute
```
Use `--to-latest` instead of `--to-earliest` to set the offset to the end of the topic.    
Use `--shift-by ` to shift the offset by positive or negative number of skips.    
You can use format `--topic <topicname>:<partition>` to address a specific partition, e.g. `--topic test-topic:0`.