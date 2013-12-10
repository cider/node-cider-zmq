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

var Q            = require('q')
  , zmq          = require('zmq')
  , EventEmitter = require('events').EventEmitter;

var codec = require('../codec');

//------------------------------------------------------------------------------
// class PubsubService
//------------------------------------------------------------------------------

var PubsubService = Object.create(EventEmitter.prototype);

/*
 * Constants
 */

const HEADER     = 'CDR#PUBSUB@01'
    , HELLO      = 0x01
    , OHAI       = 0x02
    , SYNC       = 0x03
    , EVENT      = 0x04;

const FRAME_EMPTY  = new Buffer(0)
    , FRAME_HEADER = new Buffer(HEADER)
    , FRAME_HELLO  = new Buffer([HELLO])
    , FRAME_OHAI   = new Buffer([OHAI])
    , FRAME_SYNC   = new Buffer([SYNC])
    , FRAME_EVENT  = new Buffer([EVENT]);

/*
 * PubsubService constructor fuction
 */

function createPubsubService(opts) {
  var srv = Object.create(PubsubService);

  var identity    = opts.identity    || process.env['CIDER_IDENTITY']
    , pubEndpoint = opts.pubEndpoint || process.env['CIDER_PUB_ENDPOINT']
    , pubSndhwm   = opts.pubSndhwm   || process.env['CIDER_PUB_SNDHWM']
    , subEndpoint = opts.subEndpoint || process.env['CIDER_SUB_ENDPOINT']
    , subRcvhwm   = opts.subRcvhwm   || process.env['CIDER_SUB_RCVHWM'];

  if (identity === undefined)    return Q.reject('Cider identity not set');
  if (pubEndpoint === undefined) return Q.reject('Cider PUB endpoint not set');
  if (subEndpoint === undefined) return Q.reject('Cider SUB endpoint not set');

  // Create all necessary 0MQ sockets.
  var pubSocket, subSocket;
  try {
    // Set up the pub socket.
    pubSocket = zmq.socket('dealer');
    pubSocket.setsockopt(zmq.ZMQ_IDENTITY, new Buffer(identity));
    if (pubSndhwm !== undefined) sock.setsockopt(zmq.ZMQ_SNDHWM, parseInt(pubSndhwm));

    // Set up the sub socket.
    subSocket = zmq.socket('sub');
    if (subRcvhwm !== undefined) sock.setsockopt(zmq.ZMQ_RCVHWM, parseInt(subRcvhwm));
  } catch (err) {
    return Q.reject(err);
  }

  pubSocket.on('message', srv._handlePubMessage);
  pubSocket.on('error', function(err) {
    self.emit('error', err);
  });

  subSocket.on('message', srv._handleSubMessage);
  subSocket.on('error', function(err) {
    self.emit('error', err);
  });

  srv._pubSocket = pubSocket;
  srv._subSocket = subSocket;
  srv._logger    = logger;
  srv._emitter   = new EventEmitter();
  srv._seqNums   = {};

  this._promise = Q.defer();
  return this._promise;
};

/*
 * PubsubService._sendHello
 */

PubsubService._sendHello = function _sendHello() {
  try {
    this._pubSocket.send([
        FRAME_EMPTY,
        FRAME_HEADER,
        FRAME_HELLO
    ]);
  } catch (err) {
    this._promise.reject(err);
  }
};

/*
 * PubsubService._handleOhai
 */

PubsubService._handleOhai = function _handleOhai(opts) {
  var encoding = 'json';
  if (opts['Encoding'] !== undefined) {
    encoding = opts['Encoding'].toString();
  }

  try {
    this._codec = codec.getCodec(encoding);
  } catch (err) {
    this._promise.reject(err);
    return;
  }

  this._promise.resolve(this);
};

/*
 * PubsubService.publish
 */

PubsubService.publish = function publish(eventKind, eventObject) {
  this._pubSocket.send([
      eventKind,
      FRAME_HEADER,
      FRAME_EVENT,
      this._codec.encode(eventObject)
  ]);
};

/*
 * PubsubService.subscribe
 */

PubsubService.subscribe = function(eventKind, handler) {
  // Register the event handler.
  this._emitter.on(eventKind, handler);

  // Subscribe for the requested event kind.
  this._subSocket.setsockopt(zmq.ZMQ_SUBSCRIBE, new Buffer(eventKind));

  // Send SYNC to get the current event sequence number.
  this._pubSocket.send([
      eventKind,
      FRAME_HEADER,
      FRAME_SYNC
  ]);
};

/*
 * PubsubService._handleSyncReply
 */

var maxUInt16 = Math.pow(2, 16) - 1;

PubsubService._handleSyncReply = function _handleSyncReply(eventKind, seq) {
    var lastSeq = this._seqNums[kind];

    // Update the current sequence number.
    this._seqNums[kind] = seq;

    // No event received before the acknowledgement.
    if (lastSeq === undefined) return;

    if (seq !== (lastSeq + 1) % (maxUInt16 + 1)) {
      // Sequence numbers do not match.
      err = new Error('Missed event detected');
      err.lastSeq = lastSeq;
      err.seq = seq;
      this._emitter.emit(kind, err);
    }
};

/*
 * PubsubService.unsubscribe
 */

PubsubService.unsubscribe = function(eventKind, handler) {
  // Unregister relevant event handlers.
  if (handler === undefined) this._emitter.removeAllListeners(eventKind);
  else this._emitter.removeListener(eventKind, handler);

  // Cancel the subscription if necessary.
  if (this._emitter.listeners(eventKind).length === 0) {
    this._sock.unsubscribe(eventKind);
    delete this._seqNums[eventKind];
  }
};

/*
 * PubsubService._handleEvent
 */

PubsubService._handleEvent = function handleEvent(eventKind, seq, eventObject) {
  var lastSeq = this._seqNums[eventKind];

  // Update the current sequence number.
  this._seqNums[eventKind] = seq;

  var err = null;
  if (lastSeq !== undefined && seq !== (lastSeq + 1) % (maxUInt16 + 1)) {
    // Sequence numbers do not match.
    err = new Error('Missed event detected');
    err.lastSeq = lastSeq;
    err.seq = seq;
  }

  // Invoke the registered event handlers.
  this._emitter.emit(eventKind, err, seq, eventObject);
};

/*
 * PubsubService._handlePubMessage
 */

PubsubService._handlePubMessage = function _handlePubMessage() {
  var msg = arguments;

  if (msg.length !== 4) {
    this._logger.warning('Invalid PUB message received: Invalid message length');
    return;
  }

  if (msg[1].toString() !== HEADER) {
    this._logger.warning('Invalid PUB message received: Header does not match');
    return;
  }

  if (msg[2].length !== 2) {
    this._logger.warning('Invalid PUB message received: Invalid sequence number');
    return;
  }

  var eventKind = msg[0].toString()
    , eventSeq
    , eventObject;
  try {
    eventSeq = msg[2].readUInt16BE(0);
    eventObject = this._codec.decode(msg[3]);
  } catch (err) {
    this._logger.warning('Invalid PUB message received: Invalid event encoding');
    return;
  }

  this._handleEvent(eventKind, eventSeq, eventObject);
};

/*
 * PubsubService._handleSubMessage
 */

PubsubService._handleSubMessage = function _handleSubMessage() {

};

//------------------------------------------------------------------------------
// Exports
//------------------------------------------------------------------------------

module.exports.createService = createPubsubService;
