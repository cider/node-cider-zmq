/*
  Copyright (c) 2013 Ondřej Kupka

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"),
  to deal in the Software without restriction, including without limitation
  the rights to use, copy, modify, merge, publish, distribute, sublicense,
  and/or sell copies of the Software, and to permit persons to whom
  the Software is furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.
*/

var EventEmitter = require('events').EventEmitter
  , Q            = require('q');

var loggingService = require('./services/logging')
  , pubsubService  = require('./services/pubsub')
  , rpcService     = require('./services/rpc');

//------------------------------------------------------------------------------
// class Socket
//------------------------------------------------------------------------------

var Socket = Object.create(EventEmitter.prototype);

/*
 * Constants
 */

const STATE_CREATED   = 1
    , STATE_CONNECTED = 2
    , STATE_CLOSED    = 3;

/*
 * Socket constructor function
 */

function createSocket() {
  var socket = Object.create(Socket);
  socket._state = STATE_CREATED;
  return socket;
};

/*
 * Socket.connect
 */

Socket.connect = function connect(opts) {
  var self = this;

  if (self._state != STATE_CREATED) return Q.reject('Socket already connected once');

  return loggingService.connect(opts)
  .then(function(logger) {
    return Q.allSettled([
      rpcService.connect(logger, opts),
      pubsubService.connect(logger, opts),
      Q(logger)
    ])
  ])
  .then(function(services) {
    self._rpc    = services[0];
    self._pubsub = services[1];
    self._logger = services[2];

    self._rpc.on('error', function(err) {
      self.emit('error', err);
    });
    self._pubsub.on('error', function(err) {
      self.emit('error', err);
    });
    self._logging.on('error', function(err) {
      self.emit('error', err);
    });

    self._state = STATE_CONNECTED;
    return self;
  });
};

/*
 * Socket.registerMethod
 */

Socket.register =
Socket.registerMethod = function registerMethod(method, handler) {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  this._rpc.registerMethod(method, handler);
};

/*
 * Socket.unregisterMethod
 */

Socket.unregister =
Socket.unregisterMethod = function unregisterMethod(method) {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  this._rpc.unregisterMethod(method);
};

/*
 * Socket.callMethod
 */

Socket.call =
Socket.callMethod = function callMethod(method, args) {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  // Returns a request object, which can then be sent.
  return this._rpc.callMethod(method, args);
};

/*
 * Socket.publish
 */

Socket.publish = function publish(eventKind, eventObject) {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  this._pubsub.publish(eventKind, eventObject);
};

/*
 * Socket.subscribe
 */

Socket.subscribe = function subscribe(eventKind, handler) {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  this._pubsub.subscribe(eventKind, handler);
};

/*
 * Socket.unsubscribe
 */

Socket.unsubscribe = function unsubscribe(eventKind, handler) {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  this._pubsub.unsubscribe(eventKind, handler);
};

/*
 * Socket.getLogger
 */

Socket.getLogger = function getLogger() {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');
  return this._logger;
};

/*
 * Socket.close
 */

Socket.close = function close() {
  if (this._state != STATE_CONNECTED) throw new Error('Socket not connected');

  var self = this;

  self._state = STATE_CLOSING;
  return Q.allSettled([
      self._rpc.close();
      self._pubsub.close();
      self._logger.close();
  ])
  .then(function() {
    self._state = STATE_CLOSED;
  })
};
