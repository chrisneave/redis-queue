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
      // Act
      queue.submit(submit_queue, message_key, message);

      // Assert
      expect(spy.calledOnce).to.be.ok();
    });

    it('passes the name of the destination queue to evalsha', function() {
      // Arrange
      // Act
      queue.submit(submit_queue, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(submit_queue) >= 0).to.be.ok();
    });

    it('passes the name unique message key to evalsha', function() {
      // Arrange
      // Act
      queue.submit(submit_queue, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(message_key) >= 0).to.be.ok();
    });

    it('passes the result of issuing the TIME command to evalsha', function() {
      // Arrange
      // Act
      queue.submit(submit_queue, message_key, message);

      // Assert
      expect(spy.args[0][7].getTime() === js_time.getTime()).to.be.ok();
    });

    it('passes a JSON stringified message to evalsha as an argument', function() {
      // Arrange
      // Act
      queue.submit(submit_queue, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(JSON.stringify(message)) >= 0).to.be.ok();
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
      // Act
      queue.receive(submit_queue, function(err, result) {
        // Assert
        expect(spy.calledOnce).to.be.ok();
      });
    });

    it('passes the sha1 hash returned from LOAD SCRIPT to evalsha', function() {
      // Arrange
      var lua_hash = 'abc123xyz';

      queue['_script_hash'] = { receive_message: lua_hash };

      // Act
      queue.receive(submit_queue);

      // Assert
      expect(spy.args[0][0] === lua_hash).to.be.ok();
    });

    it('passes the correct number of keys to EVALSHA', function() {
      // Arrange
      // Act
      queue.receive(submit_queue);

      // Assert
      expect(spy.args[0][1] === 2).to.be.ok();
    });

    it('passes the name of the submit queue to evalsha', function() {
      // Arrange
      // Act
      queue.receive(submit_queue);

      // Assert
      expect(spy.args[0][2] === submit_queue).to.be.ok();
    });

    it('passes the name of the receive queue to evalsha', function() {
      // Arrange
      // Act
      queue.receive(submit_queue, receive_queue);

      // Assert
      expect(spy.args[0][3] === receive_queue).to.be.ok();
    });

    it('passes the result of issuing the TIME command to evalsha', function() {
      // Arrange
      // Act
      queue.receive(submit_queue);

      // Assert
      expect(spy.args[0][4].getTime() === js_time.getTime()).to.be.ok();
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
  });
});
