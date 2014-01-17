'use strict';
// Disable the 'is defined but never used' rule which will show up on stubs.
/* jshint -W098 */

var expect = require('expect.js');
var sinon = require('sinon');
var redis_queue = require('../lib/redis-queue.js');
var Queue = redis_queue.Queue;
var ArgumentException = redis_queue.ArgumentException;
var util = require('util');

describe('Queue', function() {
  var client;
  var redis_time = [1389535019, 616092];
  var js_time = new Date(1970, 0, 1, 0, 0, 0, 1389535019616);

  beforeEach(function() {
    client = {};
    util.inherits(client, require('redis').RedisClient);
    client.time = function() {};
    client.evalsha = function() {};
    client.server_info = {redis_version: '2.3.0'}
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
        expect(e).to.be.a(ArgumentException);
      });
    });
  });

  describe('#submit', function() {
    it('calls evalsha once', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.calledOnce).to.be.ok();
    });

    it('passes the name of the destination queue to evalsha', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, [1389535019, 616092]);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(queue_name) >= 0).to.be.ok();
    });

    it('passes the name unique message key to evalsha', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, redis_time);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(message_key) >= 0).to.be.ok();
    });

    it('issues the TIME command once', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          stub = sinon.spy(client, 'evalsha'),
          spy = sinon.stub(client, 'time', function(callback) {
            callback(undefined, redis_time);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.calledOnce).to.be.ok();
    });

    it('passes the result of issuing the TIME command to evalsha', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, redis_time);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0][7].getTime() === js_time.getTime()).to.be.ok();
    })

    it('passes a JSON stringified message to evalsha as an argument', function() {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          spy = sinon.spy(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, redis_time);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      // Act
      queue.submit(queue_name, message_key, message);

      // Assert
      expect(spy.args[0].indexOf(JSON.stringify(message)) >= 0).to.be.ok();
    });

    it('invokes the callback with an undefined error after a successful submission', function(done) {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          evalsha_stub = sinon.stub(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, redis_time);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      evalsha_stub.yields();

      // Act
      queue.submit(queue_name, message_key, message, function(err, result) {
        // Assert
        expect(err).not.to.be.ok();
        done();
      });
    });

    it('invokes the callback with the new message ID after a successful submission', function(done) {
      // Arrange
      var queue_name = 'my_queue',
          message = { foo: 'bar' },
          evalsha_stub = sinon.stub(client, 'evalsha'),
          stub = sinon.stub(client, 'time', function(callback) {
            callback(undefined, redis_time);
          }),
          message_key = '123:abc',
          queue = new Queue(client);

      evalsha_stub.yields(undefined, [1337]);

      // Act
      queue.submit(queue_name, message_key, message, function(err, result) {
        // Assert
        expect(result[0]).to.equal(1337);
        done();
      });
    });
  });

  describe('#receive', function() {
  });

  describe('#finish', function() {
  });
});
