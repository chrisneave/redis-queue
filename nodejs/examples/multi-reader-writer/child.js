var redis = require('redis');
var client = redis.createClient(6379, '192.168.33.12');

console.log('Child process started => pid = %d', process.pid);

process.on('message', function(message) {
  var duration = Math.floor(Math.random() * 1000) + 1;
  console.log('%d processing work %s in %dms', process.pid, message, duration);
  setTimeout(function() {
    client.rpoplpush(process.pid, 'finished_ok', function() {
      process.send({pid: process.pid, work: message});
    });
  }, duration);
});

process.on('exit', function() {
  process.send({message: 'Child process ended'});
});
