/* Copyright 2014 Chris Neave (chrispneave@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var utils = require(__dirname + '/utils');
var exceptions = require(__dirname + '/exceptions');

var Queue = function(client, options) {
  // TODO: Need a better method of determining whether client is a RedisClient.
  if (!client.server_info || !client.server_info.redis_version) {
    throw new exceptions.ArgumentException();
  }

  this._client = client;
  this._scripts = {};
  if (options) {
    this._scripts.send = options['send_script_hash'];
    this._scripts.receive = options['receive_script_hash'];
    this._scripts.finish = options['finish_script_hash'];
  }
};

Queue.prototype.submit = function(queue_name, message_key, message, callback) {
  var self = this;

  self._client.time(function(err, result) {
    var time = utils.redisTimeToJSDate(result);
    self._client.evalsha(self._scripts.send, 4, 'message:id', 'message:received', message_key, queue_name, JSON.stringify(message), time, callback);
  });
};

Queue.prototype.receive = function(submit_queue, receive_queue, callback) {
  var self = this;

  self._client.time(function(err, result) {
    var time = utils.redisTimeToJSDate(result);
    self._client.evalsha(self._scripts.receive, 2, submit_queue, receive_queue, time, callback);
  });
};

Queue.prototype.finish = function(receive_queue, finish_queue, message_id, status, callback) {
  var self = this;

  self._client.time(function(err, result) {
    var time = utils.redisTimeToJSDate(result);
    self._client.evalsha(self._scripts.finish, 4, receive_queue, finish_queue, message_id, 'message:received', status, time, callback);
  });
};

module.exports = Queue;
