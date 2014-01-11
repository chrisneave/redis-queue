var fs = require('fs');
var redis = require('redis');
var client = redis.createClient('6379', '192.168.33.12');
var iterations = 1000;

// Redis keys
// ----------
//
// message:keys
//   SET containing all running message keys.
// message:id
//   KEY containing an icremental value used to generated a unique counter
//       key for each message.
// queue:submitted
// queue:received
// queue:finished_ok
// queue:finished_with_error

function loadScript(filename, done) {
  var lua = fs.readFileSync(filename, 'utf8');

  client.script('load', lua, function(err, result) {
    done(err, result);
  });
}

var send_message_hash;


function postMessageWithLua(message, next) {
  client.time(function(err, result) {
    var args = [
      send_message_hash,
      3,
      'message:id',
      'message:received',
      message.job_code + "." + message.job_type,
      message.body, // Don't JSON.Stringify() as this is already done by the caller.
      result[0] + '.' + result[1]
    ];

    client.evalsha(args, function(err, result) {
      if (err) throw err;
      if (!result) return process.exit();
      if (result === iterations) {
        var elapsed_ms = new Date() - started;
        console.log('Took %d ms to send %d messages - %d messages/second', elapsed_ms, iterations, Math.round(iterations / (elapsed_ms / 1000)));
        client.end();
        return process.exit();
      }
    });
  });
}

// Post a new message request
function postMessage(message, next) {
  var multi = client.multi();

  client.hmget('message', 'job_code', 'job_type', function(err, result) {
    if (result[0] && result[0] === message.job_code && result[1] === message.job_type) {
      return console.log('Duplicate message %s:%s', message.job_code, message.job_type);
    }

    multi.incr('message_id');
    multi.time();
    multi.exec(function(err, results) {
      message.id = results[0];
      message.requested_at = results[1][0];
      message.status = 'submitted';

      client.multi()
        .hmset('message:' + message.id, message)
        .lpush('queued', message.id)
        .exec(function(err, results) {
          if (message.id === iterations) {
            var elapsed_ms = new Date() - started;
            console.log('Took %d ms to send %d messages - %d messages/second', elapsed_ms, iterations, Math.round(iterations / (elapsed_ms / 1000)));
            client.end();
            return process.exit();
          }
        });
    });
  });
}

var readMessage = function() {
  client.brpoplpush('queued', 'received', 1, function(err, result) {
    if (!result) {
      var elapsed_ms = new Date() - started - 1000;
      if (messages_received === 0) {
        console.log('No messages received');
      } else {
        console.log('Took %d ms to received %d messages - %d messages/second', elapsed_ms, messages_received, Math.round(messages_received / (elapsed_ms / 1000)));
      }
      client.end();
      return process.exit();
    }

    messages_received++;
    var message_id = result[1];

    client.time(function(err, result) {
      client.multi()
        .hmset('message:' + message_id, {status: 'processing', started_at: result[0]})
        .hget('message:' + message_id, 'body')
        .exec(function(err, results) {
          //var body = JSON.parse(results[1]);
          client.time(function(err, result) {
            client.hmset('message:' + message_id, {status: 'finished ok', finished_at: result[0]});
          });
        });
      });

    readMessage();
  });
}

var started = new Date();
var messages_received = 0;

function sendLoop(send_function) {
  client.flushdb();

  for (var i = 0; i < iterations; i++) {
    send_function({
      job_type: 'test',
      job_code: 'code' + i,
      body: JSON.stringify({field: i})
    });
  }
}

switch (process.argv[2]) {
  case '-r':
    readMessage();
    break;

  case '-s':
    console.log('Sending %d messages using client commands', iterations);
    sendLoop(postMessage);
    break;

  case '-sl':
    console.log('Sending %d messages using Lua script', iterations);
    client.script('flush', function() {
      loadScript(__dirname + '/send_message.lua', function(err, result) {
        send_message_hash = result;
        sendLoop(postMessageWithLua);
      });
    });
    break;

  default: {
    console.error('Invalid arguments passed');
    return process.exit();
  }
}
