'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var rq = require(__dirname + '/../index');
var Queue = rq.Queue;
var exceptions = rq.Exceptions;
var util = require('util');

describe('Queue', function() {
  var client;
  var redis_time = [1389535019, 616092];
  var js_time = new Date(1970, 0, 1, 0, 0, 0, 1389535019616);
  var submit_queue = 'my_submit_queue';
  var receive_queue = 'my_receive_queue';
  var message = { foo: 'bar' };
  var message_key = '123:abc';
  var spy;
  var stub;
  var queue;

  beforeEach(function() {
    client = {};
    util.inherits(client, require('redis').RedisClient);
    client.time = function() {};
    client.evalsha = function() {};
    client.script = function() {};
    client.llen = function() {};
    client.server_info = {redis_version: '2.3.0'};

    spy = sinon.spy(client, 'evalsha');

    stub = sinon.stub(client, 'time', function(callback) {
      callback(undefined, redis_time);
    });

    queue = new Queue(client);
  });

  afterEach(function() {
    client.evalsha.restore();
    client.time.restore();
  });

  describe('#ctor', function() {
    it('accepts a Redis client as a parameter', function() {
      // Arrange

      // Act
      var queue = new Queue(client);

      // Assert
      expect(queue).to.be.ok();
    });

    it('throws an ArgumentException when passed an empty object', function() {
      // Arrange
      var client = {};

      expect(function() {
        // Act
        var queue = new Queue(client);
      }).to.throwException(function(e) {
        // Assert
        expect(e).to.be.an(exceptions.ArgumentException);
      });
    });

    it('stores the send_hash value from the options object in the script hash', function() {
      // Arrange
      var lua_hash = 'myhashvalue';

      // Act
      var queue = new Queue(client, {send_script_hash: lua_hash});

      // Assert
      expect(queue._scripts.send).to.equal(lua_hash);
    });

    it('stores the receive_hash value from the options object in the script hash', function() {
      // Arrange
      var lua_hash = 'myhashvalue';

      // Act
      var queue = new Queue(client, {receive_script_hash: lua_hash});

      // Assert
      expect(queue._scripts.receive).to.equal(lua_hash);
    });

    it('stores the receive_hash value from the options object in the script hash', function() {
      // Arrange
      var lua_hash = 'myhashvalue';

      // Act
      var queue = new Queue(client, {finish_script_hash: lua_hash});

      // Assert
      expect(queue._scripts.finish).to.equal(lua_hash);
    });
  });

  describe('#submit', function() {
    it('issues the TIME command once', function() {
      // Arrange
      // Act
      queue.submit(submit_queue, message_key, message);

      // Assert
      expect(stub.calledOnce).to.be.ok();
    });

    it('calls evalsha once', function() {
      // Arrange
      var lua_hash = 'abc123xyz',
          submit_queue = 'queue:submitted',
          message_key = 'message:id',
          message = {field: '123'},
          callback = function() {};
      queue['_scripts'] = { send: lua_hash };

      // Act
      queue.submit(submit_queue, message_key, message, callback);

      // Assert
      expect(spy.calledWithExactly(lua_hash, 4, 'message:id', 'message:received', message_key, submit_queue, JSON.stringify(message), js_time, callback)).to.be.ok();
    });

    it('invokes the callback with an undefined error after a successful submission', function(done) {
      // Arrange
      client.evalsha.restore();
      var evalsha_stub = sinon.stub(client, 'evalsha');

      evalsha_stub.yields();

      // Act
      queue.submit(submit_queue, message_key, message, function(err, result) {
        // Assert
        expect(err).not.to.be.ok();
        done();
      });
    });

    it('invokes the callback with the new message ID after a successful submission', function(done) {
      // Arrange
      client.evalsha.restore();
      var evalsha_stub = sinon.stub(client, 'evalsha');

      evalsha_stub.yields(undefined, [1337]);

      // Act
      queue.submit(submit_queue, message_key, message, function(err, result) {
        // Assert
        expect(result[0]).to.equal(1337);
        done();
      });
    });
  });

  describe('#receive', function() {
    it('issues the TIME command once', function() {
      // Arrange
      // Act
      queue.receive(submit_queue);

      // Assert
      expect(stub.calledOnce).to.be.ok();
    });

    it('calls evalsha once', function() {
      // Arrange
      var lua_hash = 'abc123xyz',
          submit_queue = 'queue:submitted',
          receive_queue = 'queue:received',
          callback = function() {};
      queue['_scripts'] = { receive: lua_hash };

      // Act
      queue.receive(submit_queue, receive_queue, callback);

      // Assert
      expect(spy.calledWithExactly(lua_hash, 2, submit_queue, receive_queue, js_time, callback)).to.be.ok();
    });

    it('invokes the callback with an undefined error after a successfully receiving a message', function(done) {
      // Arrange
      client.evalsha.restore();
      var evalsha_stub = sinon.stub(client, 'evalsha');

      evalsha_stub.yields();

      // Act
      queue.receive(submit_queue, receive_queue, function(err, result) {
        // Assert
        expect(err).not.to.be.ok();
        done();
      });
    });

    it('invokes the callback with the message received from the submit queue', function(done) {
      // Arrange
      client.evalsha.restore();
      var evalsha_stub = sinon.stub(client, 'evalsha');

      evalsha_stub.yields(undefined, message);

      // Act
      queue.receive(submit_queue, receive_queue, function(err, result) {
        // Assert
        expect(result).to.equal(message);
        done();
      });
    });
  });

  describe('#finish', function() {
    it('issues the TIME command once');
    it('calls evalsha once', function() {
      // Arrange
      var lua_hash = 'abc123xyz',
          receive_queue = 'queue:receive',
          finish_queue = 'queue:finished_ok',
          message_id = 'message_id:123',
          status = 'finished ok',
          callback = function() {};
      queue['_scripts'] = { finish: lua_hash };

      // Act
      queue.finish(receive_queue, finish_queue, message_id, status, callback);

      // Assert
      expect(spy.calledWithExactly(lua_hash, 4, receive_queue, finish_queue, message_id, 'message:received', status, js_time, callback)).to.be.ok();
    });
  });

  describe('#getQueueLength', function() {
    it('returns the length of the given queue', function(done) {
      // Arrange
      var err;
      var queue_length = 10;
      var queue_name = 'my_queue';
      stub = sinon.stub(client, 'llen');
      stub.withArgs(queue_name).yields(err, queue_length);

      // Act
      queue.getQueueLength(queue_name, function(err, result) {
        // Assert
        expect(result).to.equal(queue_length);
        done();
      });
    });

    it('returns the length of the given queue', function(done) {
      // Arrange
      var err;
      var queue_name = 'my_queue';
      stub = sinon.stub(client, 'llen');
      stub.withArgs('my_queue2').yields(err, 10);
      stub.withArgs('my_queue').yields(err, 0);

      // Act
      queue.getQueueLength(queue_name, function(err, result) {
        // Assert
        expect(result).to.equal(0);
        done();
      });
    });

    it('accepts multiple queue name parameters and returns the length of each as an array', function(done) {
      // Arrange
      var err;
      stub = sinon.stub(client, 'llen');
      stub.withArgs('my_queue1').yields(err, 1);
      stub.withArgs('my_queue2').yields(err, 3);
      stub.withArgs('my_queue3').yields(err, 5);
      var expected = [1, 3, 5];

      // Act
      queue.getQueueLength(['my_queue1', 'my_queue2', 'my_queue3'], function(err, result) {
        // Assert
        expect(result).to.eql(expected);
        done();
      });
    });
  });
});
