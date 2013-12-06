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

var zmq = require('zmq');

//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------

const HEADER                = 'CDR01'
    , MSGTYPE_HELLO         = 0x00
    , MSGTYPE_OHAI          = 0x01
    , MSGTYPE_KTHXBYE       = 0x02
    , MSGTYPE_EXPORT        = 0x03
    , MSGTYPE_REQUEST       = 0x04
    , MSGTYPE_INTERRUPT     = 0x05
    , MSGTYPE_PROGRESS      = 0x06
    , MSGTYPE_STREAM_FRAME  = 0x07
    , MSGTYPE_REPLY         = 0x08
    , MSGTYPE_SUBSCRIBE     = 0x09
    , MSGTYPE_SUBSCRIBE_ACK = 0x0A
    , MSGTYPE_UNSUBSCRIBE   = 0x0B
    , MSGTYPE_EVENT         = 0x0C
    , MSGTYPE_PING          = 0x0D
    , MSGTYPE_PONG          = 0x0E;

const FRAME_EMPTY                 = new Buffer(0)
    , FRAME_HEADER                = new Buffer(HEADER)
    , FRAME_MSGTYPE_HELLO         = new Buffer([MSGTYPE_HELLO])
    , FRAME_MSGTYPE_OHAI          = new Buffer([MSGTYPE_OHAI])
    , FRAME_MSGTYPE_KTHXBYE       = new Buffer([MSGTYPE_KTHXBYE])
    , FRAME_MSGTYPE_EXPORT        = new Buffer([MSGTYPE_EXPORT])
    , FRAME_MSGTYPE_REQUEST       = new Buffer([MSGTYPE_REQUEST])
    , FRAME_MSGTYPE_INTERRUPT     = new Buffer([MSGTYPE_INTERRUPT])
    , FRAME_MSGTYPE_PROGRESS      = new Buffer([MSGTYPE_PROGRESS])
    , FRAME_MSGTYPE_STREAM_FRAME  = new Buffer([MSGTYPE_STREAM_FRAME])
    , FRAME_MSGTYPE_REPLY         = new Buffer([MSGTYPE_REPLY])
    , FRAME_MSGTYPE_SUBSCRIBE     = new Buffer([MSGTYPE_SUBSCRIBE])
    , FRAME_MSGTYPE_SUBSCRIBE_ACK = new Buffer([MSGTYPE_SUBSCRIBE_ACK])
    , FRAME_MSGTYPE_UNSUBSCRIBE   = new Buffer([MSGTYPE_UNSUBSCRIBE])
    , FRAME_MSGTYPE_EVENT         = new Buffer([MSGTYPE_EVENT])
    , FRAME_MSGTYPE_PING          = new Buffer([MSGTYPE_PING])
    , FRAME_MSGTYPE_PONG          = new Buffer([MSGTYPE_PONG]);

//------------------------------------------------------------------------------
// class Socket
//------------------------------------------------------------------------------

var Socket = {}

/*
 * Socket constructor function
 */

function createSocket() {
  return Object.create(Socket);
};

/*
 * Socket.setIdentity
 */

Socket.setIdentity = function setIdentity(identity) {
  this._identity = identity;
};

// Handshake -------------------------------------------------------------------

/*
 * Socket._sendHello
 */

Socket._sendHello = function sendHello() {

};

/*
 * Socket._handleOhai
 */

Socket._handleOhai = function handleOhai(opts, next) {
  var encoding = 'json';
  if (opts['Encoding'] !== undefined) {
    encoding = opts['Encoding'].toString();
  }

  try {
    this.sock._codec = require('./codec').codec(encoding);
  } catch (err) {
    this.sock.close();
    next(err);
  }

  this.sock._state = STATE_CONNECTED;
  next(null);
};

/*
 * Socket._handlePing
 */

Socket._handlePing = function handlePing() {

};
