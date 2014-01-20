var client = require('redis').createClient(6379, '192.168.33.12');
var max_items = 5;
var queue = require(__dirname + '/lib/redis-queue').create(client, 'queued');

for (var i = 1; i < max_items + 1; i++) {
  //client.lpush('queued', 'item:' + i);
  queue.push('item:' + i);
}

console.log('Pushed %d items to the queue', i);
client.quit();
