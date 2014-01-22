var redis = require('redis');
var start = new Date();
var total_items_read = 0;

var blocking_read = function (id, next, redis_client, items_read) {
  redis_client.brpoplpush('queued', 'received', 1, function(err, result) {
    if (result) {
      items_read++;
      total_items_read++;
      return next(id, blocking_read, redis_client, items_read);
    }

    console.log('[id:%d] - read %d items', id, items_read);
    redis_client.quit();
  });
};

for (var i = 1; i < 19; i++) {
  var client = redis.createClient(6379, '192.168.33.12');
  console.log('Starting read thread %d', i);
  blocking_read(i, blocking_read, client, 0);
}

process.on('exit', function() {
  var end = new Date();
  var elapsed = end - start;
  var msg_per_sec = total_items_read / (elapsed / 1000);
  console.log('Took %d ms. Rate = %d messages per second', elapsed, msg_per_sec);
});
