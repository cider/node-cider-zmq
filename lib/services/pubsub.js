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
  , Q            = require('q')
  , zmq          = require('zmq');

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

function createPubsubService(logger, opts) {
  var srv = Object.create(PubsubService);
  opts = opts || {};

  var identity     = opts.identity     || process.env['CIDER_IDENTITY']
    , endpoint     = opts.endpoint     || process.env['CIDER_ZMQ_PUBSUB_ENDPOINT']
    , dealerSndhwm = opts.dealerSndhwm || process.env['CIDER_ZMQ_PUBSUB_DEALER_SNDHWM']
    , dealerRcvhwm = opts.dealerRcvhwm || process.env['CIDER_ZMQ_PUBSUB_DEALER_RCVHWM']
    , subRcvhwm    = opts.subRcvhwm    || process.env['CIDER_ZMQ_PUBSUB_SUB_RCVHWM'];

  if (identity === undefined) return Q.reject(new Error('Cider application identity not set'));
  if (endpoint === undefined) return Q.reject(new Error('Cider PUBSUB endpoint not set'));

  var dealer;
  try {
    // Create the DEALER socket.
    dealer = zmq.socket('dealer');
    dealer.setsockopt(zmq.ZMQ_IDENTITY, new Buffer(identity));
    if (dealerSndhwm !== undefined) dealer.setsockopt(zmq.ZMQ_SNDHWM, parseInt(dealerSndhwm));
    if (dealerRcvhwm !== undefined) dealer.setsockopt(zmq.ZMQ_RCVHWM, parseInt(dealerRcvhwm));

    // Save the SUB config for later.
    srv._subRcvhwm = parseInt(subRcvhwm);
  } catch (err) {
    if (dealer !== undefined) dealer.close();
    return Q.reject(err);
  }

  // Set up DEALER event handlers.
  dealer.on('message', srv._handleDealerMessage);
  dealer.on('error',   srv._handleDealerError);
  srv._dealer = dealer;

  // Set up the remaining private fields.
  srv._logger  = logger;
  srv._emitter = new EventEmitter();
  srv._seqNums = {};

  // Initialise the promise, send HELLO and let the magic happen.
  srv._promise = Q.defer();
  srv._sendHello();
  return srv._promise;
};

// Public API ------------------------------------------------------------------

/*
 * PubsubService.publish
 *
 * Publish new event.
 */

PubsubService.publish = function publish(eventKind, eventObject) {
  this._sendEvent(eventKind, eventObject);
};

/*
 * PubsubService.subscribe
 *
 * Subscribe for particular event kind and register a handler for that event kind.
 */

PubsubService.subscribe = function subscribe(eventKind, handler) {
  // Register the event handler.
  this._emitter.on(eventKind, handler);

  // Subscribe for the requested event kind.
  try {
    this._subSocket.setsockopt(zmq.ZMQ_SUBSCRIBE, new Buffer(eventKind));
  } catch (err) {
    this._emitter.removeListener(eventKind, handler);
    throw err;
  }

  // Send SYNC to get the current event sequence number.
  this._sendSync(eventKind);
};

/*
 * PubsubService.unsubscribe
 *
 * Cancel subscription for the chosen event kind. If no handler is specified,
 * all event handlers are removed, otherwise only the specified handler is
 * removed.
 */

PubsubService.unsubscribe = function unsubscribe(eventKind, handler) {
  // Unregister relevant event handlers.
  if (handler === undefined) this._emitter.removeAllListeners(eventKind);
  else this._emitter.removeListener(eventKind, handler);

  // Cancel the subscription if necessary.
  if (this._emitter.listeners(eventKind).length !== 0) {
    this._dealer.setsockopt(zmq.ZMQ_UNSUBSCRIBE, new Buffer(eventKind));
    delete this._seqNums[eventKind];
  }
};

/*
 * PubsubService.close
 */

PubsubService.close = function close() {
  this._dealer.close();
  this._sub.close();
};

// Sending messages ------------------------------------------------------------

PubsubService._sendHello = function _sendHello() {
  this._dealer.send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_HELLO
  ]);
};

PubsubService._sendEvent = function _sendEvent(eventKind, eventObject) {
  this._dealer.send([
      eventKind,
      FRAME_HEADER,
      FRAME_EVENT,
      this._codec.encode(eventObject)
  ]);
};

PubsubService._sendSync = function _sendSync(eventKind) {
  this._dealer.send([
      eventKind,
      FRAME_HEADER,
      FRAME_SYNC
  ]);
};

// Receiving messages ----------------------------------------------------------

/*
 * PubsubService._handleDealerMessage
 *
 * FRAME 0:  header (string)
 * FRAME 1:  message type (uint16BE)
 * FRAME 2+: payload 
 */

PubsubService._handleDealerMessage = function _handleDealerMessage() {
  var msg = arguments;

  if (msg.length < 2) {
    this._logger.warning('Invalid control message received: Message too short');
    return;
  }

  if (msg[0].toString() !== HEADER) {
    this._logger.warning('Invalid control message received: Header does not match');
    return;
  }

  if (msg[1].length !== 1) {
    this._logger.warning('Invalid control message received: Invalid message type');
    return;
  }

  switch (msg[1][0]) {
    case OHAI:
      if (msg.length % 2 !== 0) {
        this._logger.warning('Invalid OHAI message received');
        return;
      }

      var opts = {};
      for (var i = 3; i < msg.length; i += 2) {
        opts[msg[i]] = msg[i+1];
      }

      this._handleOhai(opts);
      break;
    case SYNC:
      if (msg.length !== 4) {
        this._logger.warning('Invalid SYNC message received');
        return;
      }

      var eventKind = msg[2].toString()
        , eventSeq;
      try {
        eventSeq = msg[3].readUInt16BE(0);
      } catch (err) {
        this._logger.warning('Invalid SYNC message received: Failed to parse SEQ');
        return
      }

      this._handleSync(eventKind, eventSeq);
      break;
    default:
      this._logger.warning('Invalid SUB message received: Unknown message type');
  }
};

PubsubService._handleDealerError = function _handleDealerError(err) {
  if (this._promise !== undefined) this._promise.reject(err);
  this.emit('error', err);
};

/*
 * PubsubService._handlePubMessage
 *
 * FRAME 0: event kind (string)
 * FRAME 1: header (string)
 * FRAME 2: seq (uint16)
 * FRAME 3: payload (bytes, encoded object)
 */

PubsubService._handlePubMessage = function _handlePubMessage() {
  var msg = arguments;

  if (msg.length !== 4) {
    this._logger.warning('Invalid event received: Invalid message length');
    return;
  }

  if (msg[1].toString() !== HEADER) {
    this._logger.warning('Invalid event received: Header does not match');
    return;
  }

  if (msg[2].length !== 2) {
    this._logger.warning('Invalid event received: Invalid sequence number');
    return;
  }

  var eventKind = msg[0].toString()
    , eventSeq
    , eventObject;
  try {
    eventSeq = msg[2].readUInt16BE(0);
    eventObject = this._codec.decode(msg[3]);
  } catch (err) {
    this._logger.warning('Invalid event received: Invalid seq or payload encoding');
    return;
  }

  this._handleEvent(eventKind, eventSeq, eventObject);
};

PubsubService._handlePubError = function _handlePubError(err) {
  this.emit('error', err);
};

PubsubService._handleOhai = function _handleOhai(opts) {
  // Set up the SUB socket.
  var subEndpoint = opts['Sub-Endpoint'];
  if (subEndpoint === undefined) {
    this._promise.reject(new Error('SUB endpoint not received from the server'));
    return;
  }

  var sub;
  try {
    sub = zmq.socket('sub');
    if (this._subRcvhwm !== undefined) sub.setsockopt(zmq.ZMQ_RCVHWM, this._subRcvhwm);
  } catch (err) {
    this._dealer.close();
    this._promise.reject(err);
    return;
  }

  sub.on('message', this._handleSubMessage);
  sub.on('error',   this._handleSubError);
  this._sub = sub;

  // Set up the object decoder (codec).
  var encoding = 'json';
  if (opts['Encoding'] !== undefined) {
    encoding = opts['Encoding'].toString();
  }

  try {
    this._codec = codec.getCodec(encoding);
  } catch (err) {
    this._dealer.close();
    this._sub.close();
    this._promise.reject(err);
    return;
  }

  // Resolve the connection promise.
  var promise = this._promise;
  this._promise = undefined;
  promise.resolve(this);
};

var maxUInt16 = Math.pow(2, 16) - 1;

PubsubService._handleSync = function _handleSync(eventKind, seq) {
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

//------------------------------------------------------------------------------
// Exports
//------------------------------------------------------------------------------

module.exports.createService = createPubsubService;
