## replicated_log_v1

### Testing commands:
- Send the message:
  ``` curl -X POST -H "Content-Type: application/json" http://localhost:5000/messages -d '{"message": "msg1"}' ```
- Check master messages: ``` curl http://localhost:5000/messages ```
- Check secondary1 messages: ``` curl http://localhost:5001/messages ```
- Check secondary2 messages: ``` curl http://localhost:5002/messages ```

### Notes:
- ``` REPLICATION_DELAY ``` in ``` docker-compose.yml ``` simulates the delay/sleep on the Secondary