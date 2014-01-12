var fs = require('fs');
var redis = require('redis');
var client = redis.createClient('6379', '192.168.33.12');
var utils = require(__dirname + '/lib/utils');
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

var submit_queue = 'queue:submitted';
var receive_queue = 'queue:received';
var finished_ok_queue = 'queue:finished_ok';
var finished_with_error_queue = 'queue:finished_with_error';
var received_messages = 'message:received';

var handleError = function(err) {
  if (!err) { return; }
  console.error('An error occured: ' + err);
  throw err;
}

function loadScript(filename, done) {
  var lua = fs.readFileSync(filename, 'utf8');

  client.script('load', lua, function(err, result) {
    done(err, result);
  });
}

function endSend() {
  var elapsed_ms = new Date() - started;
  console.log('Took %d ms to send %d messages - %d messages/second', elapsed_ms, iterations, Math.round(iterations / (elapsed_ms / 1000)));
  client.end();
  process.exit();
}

function sendMessageWithLua(message, lua_hash) {
  client.time(function(err, result) {
    var args = [
      lua_hash,
      4,
      'message:id',
      received_messages,
      message.job_code + "." + message.job_type,
      submit_queue,
      message.body, // Don't JSON.Stringify() as this is already done by the caller.
      utils.redisTimeToJSDate(result)
    ];

    client.evalsha(args, function(err, result) {
      if (err) handleError(err);
      if (!result) return process.exit();
      if (result === iterations) {
        return endSend();
      }
    });
  });
}

// Post a new message request
function sendMessage(message) {
  var multi = client.multi();
  var concurrent_id = message.job_code + "." + message.job_type;

  client.sismember(received_messages, concurrent_id, function(err, result) {
    if (result === 1) {
      return console.log('Duplicate message %s:%s', message.job_code, message.job_type);
    }

    multi.incr('message_id');
    multi.time();
    multi.exec(function(err, results) {
      message.id = results[0];
      var m_key = 'message:' + message.id;

      client.multi()
        .hset(m_key, 'id', message.id)
        .hset(m_key, 'status', 'submitted')
        .hset(m_key, 'requested_at', utils.redisTimeToJSDate(results[1]))
        .hset(m_key, 'concurrent_id', concurrent_id)
        .hset(m_key, 'body', message.body)
        .sadd(received_messages, concurrent_id)
        .lpush(submit_queue, message.id)
        .exec(function(err, results) {
          if (message.id === iterations) {
            return endSend();
          }
        });
    });
  });
}

function endAction(action, adjustment) {
  if (!adjustment) { adjustment = 0; }
  var elapsed_ms = new Date() - started + adjustment;
  if (messages_received === 0) {
    console.log('No messages %s', action);
  } else {
    console.log('Took %d ms to %s %d messages - %d messages/second', elapsed_ms, action, messages_received, Math.round(messages_received / (elapsed_ms / 1000)));
  }
  client.end();
  return process.exit();
}

var receiveMessageWithLua = function(lua_hash) {
  client.time(function(err, result) {
    var args = [
      lua_hash,
      2,
      submit_queue,
      receive_queue,
      utils.redisTimeToJSDate(result)
    ];

    client.evalsha(args, function(err, result) {
      if (err) handleError(err);
      if (!result) {
        return endAction('received', 0);
      }
      messages_received++;
    });

    receiveMessageWithLua(lua_hash);
  });
}

var receiveMessage = function() {
  client.brpoplpush(submit_queue, receive_queue, 1, function(err, result) {
    if (!result) {
        return endAction('received', -1000);
    }

    messages_received++;
    var message_id = result[1];

    client.time(function(err, result) {
      var m_key = 'message:' + message_id;

      client.multi()
        .hset(m_key, 'status', 'received')
        .hset(m_key, 'started_at', utils.redisTimeToJSDate(result))
        .hgetall(m_key)
        .exec();
      });

    receiveMessage();
  });
}

function processMessages(message_id, finished_ok) {
  var queue = (finished_ok) ? finished_ok_queue : finished_with_error_queue,
      status = (finished_ok) ? 'finished ok' : 'finished with error',
      m_key = 'message:' + message_id,
      finished_at;

  client.time(function(err, result) {
    finished_at = utils.redisTimeToJSDate(result);

    client.multi()
      .lrem(receive_queue, 1, message_id)
      .lpush(queue, message_id)
      .hset(m_key, 'status', status)
      .hset(m_key, 'finished_at', finished_at)
      .hget(m_key, 'concurrent_id')
      .exec(function(err, results) {
        client.srem(received_messages, result[0], function(err, result) {
          console.log(results);
          if (!result) {
            return endAction('processed', 0);
          }

          messages_received++;
        });
      });

    processMessages(++message_id, !finished_ok);
  });
}

function processMessagesWithLua(lua_hash, message_id, finished_ok) {
  var queue = (finished_ok) ? finished_ok_queue : finished_with_error_queue,
      status = (finished_ok) ? 'finished ok' : 'finished with error',
      finished_at;

  client.time(function(err, result) {
    finished_at = utils.redisTimeToJSDate(result);

    var args = [
      lua_hash,
      4,
      receive_queue,
      queue,
      message_id,
      received_messages,
      status,
      finished_at
    ];

    client.evalsha(args, function(err, result) {
      if (err) handleError(err);
      if (!result) {
        return endAction('processed', 0);
      }
      messages_received++;
    });

    processMessagesWithLua(lua_hash, ++message_id, !finished_ok);
  });
}

var started = new Date();
var messages_received = 0;

function sendLoop(send_function, lua_hash) {
  client.flushdb();

  for (var i = 0; i < iterations; i++) {
    send_function({
      job_type: 'test',
      job_code: 'code' + i,
      body: JSON.stringify({field: i})
    }, lua_hash);
  }
}

switch (process.argv[2]) {
  case '-r':
    console.log('Receiving messages using client commands');
    receiveMessage();
    break;

  case '-rl':
    console.log('Receiving messages using Lua script');
    client.script('flush', function() {
      loadScript(__dirname + '/receive_message.lua', function(err, result) {
        if (err) return console.error(err);
        receiveMessageWithLua(result);
      });
    });
    receiveMessage();
    break;

  case '-s':
    console.log('Sending %d messages using client commands', iterations);
    sendLoop(sendMessage);
    break;

  case '-sl':
    console.log('Sending %d messages using Lua script', iterations);
    client.script('flush', function() {
      loadScript(__dirname + '/send_message.lua', function(err, result) {
        if (err) return console.error(err);
        sendLoop(sendMessageWithLua, result);
      });
    });
    break;

  case '-p':
    console.log('Processing %d messages using client commands', iterations);
    processMessages(1, true);
    break;

  case '-pl':
    console.log('Processing %d messages using Lua script', iterations);
    client.script('flush', function() {
      loadScript(__dirname + '/process_message.lua', function(err, result) {
        if (err) return console.error(err);
        processMessagesWithLua(result, 1, true);
      });
    });
    break;

  default: {
    console.error('Invalid arguments passed');
    return process.exit();
  }
}
