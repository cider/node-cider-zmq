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

var EventEmitter = require('events').EventEmitter;

/*---------------------------*
 * class SubscriptionService *
 *---------------------------*/

var SubscriptionService = {};

function createSubscriptionService(socket) {
  var service = Object.create(SubscriptionService);
  service._sock = socket;
  service._emitter = new EventEmitter();
  service._seqNums = {};
  return service;
}

/*
 * SubscriptionService.subscribe
 */

SubscriptionService.subscribe = function(kind, handler) {
  // Register the event handler.
  this._emitter.on(kind, handler);

  // Subscribe for the requested event kind.
  this._sock.subscribe(kind);
};

/*
 * SubscriptionService.unsubscribe
 */

SubscriptionService.unsubscribe = function(kind, handler) {
  // Unregister relevant event handlers.
  if (handler === undefined) this._emitter.removeAllListeners(kind);
  else this._emitter.removeListener(kind, handler);

  // Cancel the subscription if necessary.
  if (this._emitter.listeners(kind).length === 0) {
    this._sock.unsubscribe(kind);
    delete this._seqNums[kind];
  }
};

/*
 * SubscriptionService._handleSubscribeAck
 */

var maxUInt16 = Math.pow(2, 16) - 1;

SubscriptionService._handleSubscribeAck = function handleSubscribeAck(kind, seq) {
    var prevSeq = this._seqNums[kind];

    // Update the current sequence number.
    this._seqNums[kind] = seq;

    // No event received before the acknowledgement.
    if (prevSeq === undefined) return;

    if (seq !== (prevSeq + 1) % (maxUInt16 + 1)) {
      // Sequence numbers do not match.
      err = new Error('Missed event detected');
      err.prevSeq = prevSeq;
      err.seq = seq;
      this._emitter.emit(kind, err);
    }
};

/*
 * SubscriptionService._handleEvent
 */

SubscriptionService._handleEvent = function handleEvent(kind, seq, body) {
  var prevSeq = this._seqNums[kind];

  // Update the current sequence number.
  this._seqNums[kind] = seq;

  var err = null;
  if (prevSeq !== undefined && seq !== (prevSeq + 1) % (maxUInt16 + 1)) {
    // Sequence numbers do not match.
    err = new Error('Missed event detected');
    err.prevSeq = prevSeq;
    err.seq = seq;
  }

  // Invoke the registered event handlers.
  this._emitter.emit(kind, err, seq, body);
};

/*
 * Exports
 */

module.exports.createSubscriptionService = createSubscriptionService;
