## replicated_log_v3

### Testing commands:
- Send the message:
  ``` curl -X POST http://localhost:5000/messages   -H "Content-Type: application/json"   -d '{"message": "msg1", "w": 2}' ```
  - where w is the write concern parameter
  - w = 1 - only from master 
  - w = 2 - from master and one secondary 
  - w = 3 - from master and two secondaries
- Check master messages: ``` curl http://localhost:5000/messages ```
- Check secondary1 messages: ``` curl http://localhost:5001/messages ```
- Check secondary2 messages: ``` curl http://localhost:5002/messages ```
- Check secondaries health, quorum size, and master read-only mode status: ``` curl http://localhost:5000/health ```
- Heartbeat endpoint of secondary: ``` curl http://localhost:5001/messages ``` or ``` curl http://localhost:5002/messages ```

### Notes:
- ``` REPLICATION_DELAY ``` in ``` docker-compose.yml ``` simulates the delay/sleep on the Secondary
- ``` INTERNAL_ERROR_PROB ``` in ``` docker-compose.yml ``` is a value between 0 and 1 that represents the probability of a random internal server error, used to test the retry mechanism